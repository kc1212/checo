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
CUSTOM_STYLES = ['x-', '^--', '*:', 'o--',  's:', 'v-', 'x-.', '^:']
LINE_WIDTH = 1
Exp = enum.Enum('Exp',
                'round_duration_mean '
                'round_duration_std '
                'validation_count '
                'consensus_duration_mean '
                'consensus_duration_std '
                'message_size_cons '
                'message_size_tx '
                'message_size_tx_std ' 
                'message_size_round '
                'message_size_round_std '
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

    # we use regular array here because the third dimension is not an element, but a list
    timeseries_arr = [[None for _ in range(len(populations))] for _ in range(len(facilitators))]

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
            timeseries_backlog = TimeSeriesBacklogReader()
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
                                   backlog.read_line,
                                   timeseries_backlog.read_line])
                round_duration.finish_file(fname)
                validation_count.finish_file(fname)
                consensus_duration.finish_file(fname)
                message_size.finish_file(fname)
                backlog.finish_file(fname)
                timeseries_backlog.finish_line(fname)

            # NOTE below need to be in sync with `data_labels`
            arr[i][j][0] = round_duration.mean
            arr[i][j][1] = round_duration.std
            arr[i][j][2] = validation_count.total_rate
            arr[i][j][3] = consensus_duration.mean
            arr[i][j][4] = consensus_duration.std
            arr[i][j][5] = message_size.sum_consensus_size()
            arr[i][j][6] = message_size.mean_validation_size()
            arr[i][j][7] = message_size.stderr_validation_size()
            arr[i][j][8] = message_size.mean_round_size()
            arr[i][j][9] = message_size.stderr_round_size()

            tot_tx_count, tot_vd_count, tot_backlog = backlog.total_counts
            arr[i][j][10] = tot_tx_count
            arr[i][j][11] = tot_vd_count
            arr[i][j][12] = tot_backlog

            if population >= 1000:
                timeseries_arr[i][j] = timeseries_backlog.get_result()

    x = (arr, facilitators, populations, timeseries_arr)

    with open(os.path.join(folder_name, 'pickle'), 'w') as f:
        pickle.dump(x, f)
    return x


def load_from_cache(folder_name):
    with open(os.path.join(folder_name, 'pickle'), 'r') as f:
        x = pickle.load(f)
    return x


def filter_data(arr, facilitators, populations, timeseries_arr):
    # right now we just filter out 8 facilitators
    return arr[1:, :, :], facilitators[1:], populations, timeseries_arr[1:]


def plot(folder_name, recompute):
    """
    We have two "inputs" - population and no. of facilitator and two "outputs" - consensus time and throughput
    thus 4 graphs is needed. More inputs/outputs may be added later.
    :param folder_name: 
    :return: 
    """

    if recompute:
        arr, facilitators, populations, timeseries_arr = load_data(folder_name)
    else:
        try:
            arr, facilitators, populations, timeseries_arr = load_from_cache(folder_name)
        except IOError:
            arr, facilitators, populations, timeseries_arr = load_data(folder_name)

    # arr, facilitators, populations, timeseries_arr = filter_data(arr, facilitators, populations, timeseries_arr)

    print facilitators
    print populations

    # plot throughput vs population
    p1 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = '{}'.format(facilitator)
        plt.plot(populations, arr[i, :, Exp.validation_count.value - 1], CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Throughput (validated tx / s)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p1.savefig(os.path.join(folder_name, 'throughput-vs-population.pdf'))

    # plot round duration vs population
    p2 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = '{}'.format(facilitator)
        plt.errorbar(populations, arr[i, :, Exp.round_duration_mean.value - 1],
                     yerr=arr[i, :, Exp.round_duration_std.value - 1],
                     fmt=CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH,
                     capsize=2, elinewidth=0.5)
    plt.ylabel('Round duration (seconds)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p2.savefig(os.path.join(folder_name, 'round-duration-vs-population.pdf'))

    # plot rounds duration vs facilitators
    p3 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.errorbar(facilitators, arr[:, i, Exp.round_duration_mean.value - 1],
                     yerr=arr[:, i, Exp.round_duration_std.value - 1],
                     fmt=STYLES[i], label=legend, lw=LINE_WIDTH,
                     capsize=2, elinewidth=0.5)
    plt.ylabel('Round duration (seconds)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p3.savefig(os.path.join(folder_name, 'round-duration-vs-facilitators.pdf'))

    # plot consensus duration vs facilitators
    p4 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.consensus_duration_mean.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Consensus duration (seconds)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p4.savefig(os.path.join(folder_name, 'consensus-duration-vs-facilitators.pdf'))

    # plot consensus duration vs population
    p5 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = '{}'.format(facilitator)
        plt.plot(populations, arr[i, :, Exp.consensus_duration_mean.value - 1], CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Consensus duration (seconds)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p5.savefig(os.path.join(folder_name, 'consensus-duration-vs-population.pdf'))

    p6 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.message_size_cons.value - 1] / 1024 / 1024, STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Communication cost per round of consensus (MB)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p6.savefig(os.path.join(folder_name, 'consensus-communication-cost-vs-facilitators.pdf'))

    p7 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.plot(populations, arr[i, :, Exp.message_size_cons.value - 1] / 1024 / 1024, CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Communication cost per round of consensus (MB)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p7.savefig(os.path.join(folder_name, 'consensus-communication-cost-vs-population.pdf'))

    p8 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.errorbar(facilitators, arr[:, i, Exp.message_size_tx.value - 1],
                     yerr=arr[:, i, Exp.message_size_tx_std.value - 1],
                     fmt=STYLES[i], label=legend, lw=LINE_WIDTH,
                     capsize=2, elinewidth=0.5)
    plt.ylabel('Communication cost per validated transaction (bytes)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p8.savefig(os.path.join(folder_name, 'tx-communication-cost-vs-facilitators.pdf'))

    p9 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.errorbar(populations, arr[i, :, Exp.message_size_tx.value - 1],
                     yerr=arr[i, :, Exp.message_size_tx_std.value - 1],
                     fmt=CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH,
                     capsize=2, elinewidth=0.5)
    plt.ylabel('Communication cost per validated transactions (bytes)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p9.savefig(os.path.join(folder_name, 'tx-communication-cost-vs-population.pdf'))

    p10 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.plot(facilitators, arr[:, i, Exp.total_backlog.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Average backlog per node')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p10.savefig(os.path.join(folder_name, 'backlog-vs-facilitators.pdf'))

    p11 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.plot(populations, arr[i, :, Exp.total_backlog.value - 1], CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH)
    plt.ylabel('Average backlog per node')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p11.savefig(os.path.join(folder_name, 'backlog-vs-population.pdf'))

    p12 = plt.figure()
    for i, population in enumerate(populations):
        legend = '{}'.format(population)
        plt.errorbar(facilitators, arr[:, i, Exp.message_size_round.value - 1] / 1024 / 1024,
                     yerr=arr[:, i, Exp.message_size_round_std.value - 1] / 1024 / 1024,
                     fmt=STYLES[i], label=legend, lw=LINE_WIDTH,
                     capsize=2, elinewidth=0.5)
    plt.ylabel('Communication cost per round (MB)')
    plt.xlabel('Number of facilitators $n$')
    plt.legend(loc='upper left', title='population')
    plt.grid()
    p12.savefig(os.path.join(folder_name, 'round-communication-cost-vs-facilitators.pdf'))

    p13 = plt.figure()
    for i, facilitator in enumerate(facilitators):
        legend = str(facilitator)
        plt.errorbar(populations, arr[i, :, Exp.message_size_round.value - 1] / 1024 / 1024,
                     yerr=arr[i, :, Exp.message_size_round_std.value - 1] / 1024 / 1024,
                     fmt=CUSTOM_STYLES[i], label=legend, lw=LINE_WIDTH,
                     capsize=2, elinewidth=0.5)
    plt.ylabel('Communication cost per round (MB)')
    plt.xlabel('Population size $N$')
    plt.legend(loc='upper left', title='facilitators')
    plt.grid()
    p13.savefig(os.path.join(folder_name, 'round-communication-cost-vs-population.pdf'))

    for i, facilitator in enumerate(facilitators):
        for j, population in enumerate(populations):
            if facilitator == 32 and population == 1200:
                p14 = plt.figure()
                # 20 seconds is the timeseries interval
                x = map(lambda a: a * 20, range(len(timeseries_arr[i][j][0])))
                tx_count, vd_count = timeseries_arr[i][j]
                plt.plot(x, tx_count, 'v', label="transactions", lw=LINE_WIDTH)
                plt.plot(x, vd_count, 'o', label="validations", lw=LINE_WIDTH)
                plt.xlabel('Time (seconds)')
                plt.ylabel('Count')
                plt.legend(loc='upper left')
                plt.grid()
                p14.savefig(os.path.join(folder_name, 'timeseries.pdf'))

    plt.show()


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
        return np.std(self._differences) / np.sqrt(len(self._differences))


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
        return np.std(self._durations) / np.sqrt(len(self._durations))


class BacklogReader(object):
    def __init__(self):
        self._tx_count = 0
        self._vd_count = 0
        self._tmp_tx_count = 0
        self._tmp_vd_count = 0
        self._backlogs = []

    def read_line(self, line):
        match = 'TC: current tx count '
        if match in line:
            tmp = line.split(match)[1].split(', validated')
            self._tmp_tx_count = int(tmp[0])
            self._tmp_vd_count = int(tmp[1])
            assert self._tmp_tx_count >= self._tmp_vd_count

    def finish_file(self, fname):
        self._tx_count += self._tmp_tx_count
        self._vd_count += self._tmp_vd_count
        self._backlogs.append(self._tmp_tx_count - self._tmp_vd_count)

        self._tmp_tx_count = 0
        self._tmp_vd_count = 0

    @property
    def total_counts(self):
        return self._tx_count, self._vd_count, np.mean(self._backlogs)


class MessageSizeReader(object):
    FIELDS = ["Cons", "TxReq", "Ping", "ValidationReq", "ACS",
              "SigWithRound", "AskCons", "Pong", "CpBlock", "TxResp",
              "ValidationResp"]

    def __init__(self):
        self._consensus_sizes = []
        self._validation_sizes = []
        self._round_sizes = []
        self._tmp_consensus_size = 0
        self._tmp_validation_size = 0
        self._tmp_validation_count = 0
        self._tmp_round_size = 0
        self._max_r = 0

    def read_line(self, line):
        validation_match = 'TC: current tx count '
        if validation_match in line:
            self._tmp_validation_count = int(line.split(validation_match)[1].split(', validated')[1])

        if 'NODE: messages info' in line:
            messages_info = json.loads(line.split('messages info')[1])
            self._tmp_validation_size = self._get_validation_size(messages_info['sent'], messages_info['recv'])
            self._tmp_consensus_size = self._get_consensus_size(messages_info['sent'], messages_info['recv'])
            self._tmp_round_size = self._get_round_size(messages_info['sent'], messages_info['recv'])
        elif 'updated new promoters' in line:
            r = int(line.split('TC: round ')[1].split(',')[0])
            if r > self._max_r:
                self._max_r = r

    def finish_file(self, fname):
        if self._tmp_validation_count == 0:
            print "Nothing got validated for " + fname
        else:
            self._validation_sizes.append(float(self._tmp_validation_size) / float(self._tmp_validation_count))
        self._consensus_sizes.append(self._tmp_consensus_size)
        self._round_sizes.append(float(self._tmp_round_size) / float(self._max_r))

        self._tmp_validation_size = 0
        self._tmp_consensus_size = 0
        self._tmp_validation_count = 0
        self._tmp_round_size = 0

    @staticmethod
    def _get_consensus_size(sent_res, recv_res):
        return value_or_zero(sent_res, 'ACS') + value_or_zero(recv_res, 'ACS') + \
               value_or_zero(sent_res, 'AskCons') + value_or_zero(recv_res, 'AskCons')

    @staticmethod
    def _get_round_size(sent_res, recv_res):
        return MessageSizeReader._get_consensus_size(sent_res, recv_res) + \
               value_or_zero(sent_res, 'Cons') + value_or_zero(sent_res, 'Cons')

    @staticmethod
    def _get_validation_size(sent_res, recv_res):
        return value_or_zero(recv_res, 'ValidationResp') + value_or_zero(recv_res, 'TxResp')

    def sum_consensus_size(self):
        return np.sum(self._consensus_sizes) / self._max_r

    def mean_validation_size(self):
        return np.mean(self._validation_sizes)

    def stderr_validation_size(self):
        return np.std(self._validation_sizes) / np.sqrt(len(self._validation_sizes))

    def mean_round_size(self):
        return np.mean(self._round_sizes)

    def stderr_round_size(self):
        return np.std(self._round_sizes) / np.sqrt(len(self._round_sizes))


def value_or_zero(d, k):
    return d[k] if k in d else 0


class ValidationCountReader(object):
    def __init__(self):
        self._lines_of_interest = []
        self._rates = []

    def read_line(self, line):
        match = 'TC: current tx count '
        if match in line:
            if int(line.split(match)[1].split(', validated')[1]) == 0:
                pass
            elif len(self._lines_of_interest) >= 2:
                self._lines_of_interest[1] = line
            else:
                self._lines_of_interest.append(line)

    def finish_file(self, fname):
        assert len(self._lines_of_interest) == 2

        if not self._lines_of_interest:
            print "Nothing got validated for {}".format(fname)
            return

        start_t = datetime_from_line(self._lines_of_interest[0])
        end_t = datetime_from_line(self._lines_of_interest[-1])

        diff = difference_in_seconds(start_t, end_t)
        if diff == 0:
            print "Difference is zero for {}".format(fname)
            return

        validated_count = self._lines_of_interest[-1].split('TC: current tx count')[1].split(', validated')[1]
        rate = float(validated_count) / diff
        self._rates.append(rate)

        self._lines_of_interest = []

    @property
    def total_rate(self):
        return np.sum(self._rates)


class TimeSeriesBacklogReader(object):
    def __init__(self):
        self._all_tx_results = []
        self._all_vd_results = []
        self._tx_result = []
        self._vd_result = []
        self._is_promoter = False

    def read_line(self, line):
        if "I'm a promoter" in line:
            self._is_promoter = True

        if self._is_promoter:
            self._tx_result = []
            self._vd_result = []
            return

        match = 'TC: current tx count '
        if match in line:
            tmp = line.split(match)[1].split(', validated')
            self._tx_result.append(int(tmp[0]))
            self._vd_result.append(int(tmp[1]))

    def finish_line(self, fname):
        self._is_promoter = False
        if self._tx_result and self._vd_result:
            self._all_tx_results.append(self._tx_result)
            self._all_vd_results.append(self._vd_result)
        self._tx_result = []
        self._vd_result = []

    def get_result(self):
        tx_len = max(map(len, self._all_tx_results)) - 2
        vd_len = max(map(len, self._all_vd_results)) - 2

        assert tx_len == vd_len

        # these fail if not enough results
        tx_results = [x[:tx_len] for x in self._all_tx_results]
        vd_results = [x[:vd_len] for x in self._all_vd_results]

        return np.mean(np.array(tx_results), axis=0), np.mean(np.array(vd_results), axis=0)


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
    return (b - a).total_seconds()


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
