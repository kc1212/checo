#!/usr/bin/env python2

import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import zipfile
import dateutil
import pickle
import enum
import json
import collections


"""
Directory structure:
/root
    /<promoter1>-<population1>.zip
    /<promoter2>-<population2>.zip
    /...
# file names must be unique

After extracting
/root
    /<promoter1>-<population1>.zip
    /<promoter1>-<population1>
        /<data files>
    /<promoter2>-<population2>.zip
    /<promoter2>-<population2>
        /<data files>
"""


MARKER_STYLES = ['x', 'o', '^', 's', '*', 'v']
LINE_STYLES = ['-', '--']
STYLES = [l + m for m in MARKER_STYLES for l in LINE_STYLES]
CUSTOM_STYLES = ['x-', '^--', '*:', 'o--',  's:', 'v-', 'x-.']
LINE_WIDTH = 1
Exp = enum.Enum('Exp',
                'round_duration_mean '
                'round_duration_std '
                'validation_count '
                'consensus_duration_mean '
                'consensus_duration_std '
                'message_size_cons '
                'message_size_tx ' 
                'total_tx_count '
                'total_vd_count '
                'total_backlog')


def extract_file(file_name):
    assert len(file_name.split('.')) == 2
    assert file_name.split('.')[-1] == 'zip'
    dir_name = file_name.split('.')[0]

    try:
        os.mkdir(dir_name)
        zip_ref = zipfile.ZipFile(file_name, 'r')
        zip_ref.extractall(dir_name)
        zip_ref.close()
    except OSError as e:
        print "File already extracted, exception: {}".format(e)


def extract_files(folder_name):
    for root, dirs, files in os.walk(folder_name):
        for f in files:
            if f.split('.')[-1] == 'zip':
                full_path = os.path.join(root, f)
                print "trying to extract {}".format(full_path)
                extract_file(full_path)


def load_data(folder_name):
    """
    Assume files are already extracted, read the data files and load them in a numpy array
    :param folder_name: 
    :return: 
    """

    extract_files(folder_name)

    # iterate the directories once to see what keys we have
    facilitators = []
    populations = []
    for path in os.listdir(folder_name):
        full_path = os.path.join(folder_name, path)
        if os.path.isdir(full_path):
            facilitator, population = path.split('-')
            facilitators.append(int(facilitator))
            populations.append(int(population))

    # build empty array using the metadata we have
    facilitators = list(set(facilitators))
    facilitators.sort()

    populations = list(set(populations))
    populations.sort()

    arr = np.empty([len(facilitators), len(populations), len(Exp)])

    # iterate the directory again to actually read the data
    for i, facilitator in enumerate(facilitators):
        for j, population in enumerate(populations):
            full_path = os.path.join(folder_name, "{}-{}".format(facilitator, population))
            print "processing", full_path

            round_duration = RoundDurationReader()
            validation_count = ValidationCountReader()
            consensus_duration = ConsensusDurationReader()
            message_size = MessageSizeReader()
            backlog = BacklogReader()
            # readers = [round_duration, validation_count, consensus_duration, message_size]

            fnames = list_files_that_match(full_path)
            if len(fnames) == 0:
                print "NOTHING IN " + full_path
                continue

            assert len(fnames) == population
            for fname in fnames:
                iter_line_with_cb(fname,
                                  [round_duration.read_line,
                                   validation_count.read_line,
                                   consensus_duration.read_line,
                                   message_size.read_line,
                                   backlog.read_line])
                round_duration.finish_file(fname)
                validation_count.finish_file(fname)
                consensus_duration.finish_file(fname)
                message_size.finish_file(fname)
                backlog.finish_file(fname)

            # NOTE below need to be in sync with `data_labels`
            arr[i][j][0] = round_duration.mean
            arr[i][j][1] = round_duration.std
            arr[i][j][2] = validation_count.total_rate
            arr[i][j][3] = consensus_duration.mean
            arr[i][j][4] = consensus_duration.std
            arr[i][j][5], arr[i][j][6] = message_size.sizes

            tot_tx_count, tot_vd_count, tot_backlog = backlog.total_counts
            arr[i][j][7] = tot_tx_count
            arr[i][j][8] = tot_vd_count
            arr[i][j][9] = tot_backlog

            # communication complexity per validated transaction
            arr[i][j][6] = arr[i][j][6] / tot_vd_count

    x = (arr, facilitators, populations)

    with open(os.path.join(folder_name, 'pickle'), 'w') as f:
        pickle.dump(x, f)
    return x


def load_from_cache(folder_name):
    with open(os.path.join(folder_name, 'pickle'), 'r') as f:
        x = pickle.load(f)
    return x


def filter_data(arr, facilitators, populations):
    # right now we just filter out 8 facilitators
    return arr[1:, :, :], facilitators[1:], populations


def plot(folder_name, recompute):
    """
    We have two "inputs" - population and no. of facilitator and two "outputs" - consensus time and throughput
    thus 4 graphs is needed. More inputs/outputs may be added later.
    :param folder_name: 
    :return: 
    """

    if recompute:
        arr, facilitators, populations = load_data(folder_name)
    else:
        try:
            arr, facilitators, populations = load_from_cache(folder_name)
        except IOError:
            arr, facilitators, populations = load_data(folder_name)

    arr, facilitators, populations = filter_data(arr, facilitators, populations)
    print facilitators
    print populations
    # plot throughput vs population
    p1 = plt.figure(1)
    for i, facilitator in enumerate(facilitators):
        legend = '{}'.format(facilitator)
        plt.plot(populations, arr[i, :, Exp.validation_count.value - 1], CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Throughput (validated tx / s)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p1.show()

    # plot round duration vs population
    p2 = plt.figure(2)
    for i, facilitator in enumerate(facilitators):
        legend = '{}'.format(facilitator)
        plt.plot(populations, arr[i, :, Exp.round_duration_mean.value - 1], CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Round duration (seconds)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p2.show()

    # plot rounds duration vs facilitators
    p3 = plt.figure(3)
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.round_duration_mean.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Round duration (seconds)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p3.show()

    # plot consensus duration vs facilitators
    p4 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.consensus_duration_mean.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Consensus duration (seconds)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p4.show()

    # plot consensus duration vs population
    p5 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = '{}'.format(facilitator)
        plt.plot(populations, arr[i, :, Exp.consensus_duration_mean.value - 1], CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Consensus duration (seconds)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p5.show()

    p6 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.message_size_cons.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Communication cost per round of consensus (bytes)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p6.show()

    p7 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.plot(populations, arr[i, :, Exp.message_size_cons.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Communication cost round of consensus (bytes)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p7.show()

    p8 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.message_size_tx.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Communication cost per validated transactions (bytes)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p8.show()

    p9 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.plot(populations, arr[i, :, Exp.message_size_tx.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Communication cost per validated transactions (bytes)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p9.show()

    p10 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.total_backlog.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Average backlog per node')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p10.show()

    p11 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.plot(populations, arr[i, :, Exp.total_backlog.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Average backlog per node')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p11.show()

    # TODO backlog as time series


def list_files_that_match(folder_name, match='.err'):
    fnames = []
    for root, dirs, files in os.walk(folder_name):
        for f in files:
            if match in f:
                fnames.append(os.path.join(root, f))
    return fnames


class RoundDurationReader(object):
    def __init__(self):
        self._lines_of_interest = []
        self._differences = []

    def read_line(self, line):
        match = 'updated new promoters'
        if match in line:
            self._lines_of_interest.append(line)

    def finish_file(self, fname):
        times = [datetime_from_line(line) for line in self._lines_of_interest]
        idx = 0
        for a, b in zip(times, times[1:]):
            if "round {}".format(idx+1) in self._lines_of_interest[idx] \
                    and "round {}".format(idx+2) in self._lines_of_interest[idx+1]:
                self._differences.append(difference_in_seconds(a, b))
                idx += 1
            else:
                print "a round is skipped: round {} in file {}\n" \
                      "\t{}\n" \
                      "\t{}".format(idx, fname, self._lines_of_interest[idx+1], self._lines_of_interest[idx+2])
                break
        self._lines_of_interest = []

    @property
    def mean(self):
        return np.mean(self._differences)

    @property
    def std(self):
        return np.std(self._differences)


class ConsensusDurationReader(object):
    def __init__(self):
        self._start_time = None
        self._durations = []

    def read_line(self, line):
        if self._start_time:
            if 'ACS: DONE' in line:
                self._durations.append(difference_in_seconds(self._start_time, datetime_from_line(line)))
                self._start_time = None
        else:
            if 'ACS: initiating' in line:
                self._start_time = datetime_from_line(line)

    def finish_file(self, fname):
        self._start_time = None

    @property
    def mean(self):
        return np.mean(self._durations)

    @property
    def std(self):
        return np.std(self._durations)


class BacklogReader(object):
    def __init__(self):
        self._tx_count = 0
        self._vd_count = 0
        self._backlog = 0
        self._backlogs = []

    def read_line(self, line):
        match = 'TC: current tx count '

        if 'TC: verified' in line:
            self._vd_count += 1
        elif 'TC: added tx' in line:
            self._tx_count += 1
        elif match in line:
            tmp = line.split(match)[1].split(', validated')
            self._backlog = int(tmp[0]) - int(tmp[1])
            assert self._backlog >= 0

    def finish_file(self, fname):
        self._backlogs.append(self._backlog)
        self._backlog = 0

    @property
    def total_counts(self):
        return self._tx_count, self._vd_count, np.mean(self._backlogs)


class MessageSizeReader(object):
    FIELDS = ["Cons", "TxReq", "Ping", "ValidationReq", "ACS",
              "SigWithRound", "AskCons", "Pong", "CpBlock", "TxResp",
              "ValidationResp"]

    def __init__(self):
        self._latest_json = None
        self._collection = []
        self._max_r = 0

    def read_line(self, line):
        if 'messages info' in line:
            self._latest_json = json.loads(line.split('messages info')[1])
        elif 'TC: round ' in line:
            r = int(line.split('TC: round ')[1].split(',')[0])
            if r > self._max_r:
                self._max_r = r

    def finish_file(self, fname):
        assert self._latest_json
        self._collection.append(self._latest_json)
        self._latest_json = None

    @property
    def sizes(self):
        sent_res = collections.defaultdict(long)
        recv_res = collections.defaultdict(long)
        for j in self._collection:
            for f in self.FIELDS:
                try:
                    sent_res[f] += j['sent'][f]
                    recv_res[f] += j['recv'][f]
                except KeyError:
                    continue

        consensus_message_size = sent_res['ACS'] + recv_res['ACS'] + sent_res['AskCons'] + recv_res['AskCons']
        # tx_message_size = sent_res['TxResp'] + sent_res['TxReq'] + sent_res['ValidationReq'] + sent_res['ValidationResp']
        vd_message_size = recv_res['TxResp'] + recv_res['TxReq'] + recv_res['ValidationReq'] + recv_res['ValidationResp']

        return consensus_message_size / (self._max_r - 1), vd_message_size


class ValidationCountReader(object):
    def __init__(self):
        self._lines_of_interest = []
        self._rates = []

    def read_line(self, line):
        match = 'TC: verified'
        if match in line:
            self._lines_of_interest.append(line)

    def finish_file(self, fname):
        if not self._lines_of_interest:
            print "Nothing got validated for {}".format(fname)
            return

        start_t = datetime_from_line(self._lines_of_interest[0])
        end_t = datetime_from_line(self._lines_of_interest[-1])

        diff = difference_in_seconds(start_t, end_t)
        if diff == 0:
            print "Difference is zero for {}".format(fname)
            return

        rate = float(len(self._lines_of_interest)) / diff
        self._rates.append(rate)

        self._lines_of_interest = []

    @property
    def total_rate(self):
        return np.sum(self._rates)


class MessageReader(object):
    def __init__(self):
        pass

    def read_line(self, line):
        pass


def datetime_from_line(line):
    """
    log files are like this
    2017-04-09 21:57:02,001 - INFO ...
    we split ' - ' and take the 0th element as time
    :param line: 
    :return: 
    """
    return dateutil.parser.parse(line.split(' - ')[0])


def iter_line_with_cb(fname, cbs):
    with open(fname, 'r') as f:
        for line in f:
            for cb in cbs:
                cb(line)


def difference_in_seconds(a, b):
    """
    a and b are both datetime type
    :param a: 
    :param b: 
    :return: 
    """
    assert b > a
    return (b - a).seconds


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyse experiment results.')
    parser.add_argument(
        '--dir',
        help='directory containing the data',
    )
    parser.add_argument(
        '--recompute',
        help='recompute, do not use the cache',
        action='store_true'
    )
    args = parser.parse_args()

    plot(os.path.expandvars(args.dir), args.recompute)

    raw_input()
