#!/usr/bin/env python2

import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import zipfile
import dateutil
import pickle
import enum


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
                'consensus_duration_std')


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

            fnames = list_files_that_match(full_path)
            for fname in fnames:
                iter_line_with_cb(fname,
                                  [round_duration.read_line, validation_count.read_line, consensus_duration.read_line])
                round_duration.finish_file(fname)
                validation_count.finish_file(fname)
                consensus_duration.finish_file(fname)

            # NOTE below need to be in sync with `data_labels`
            arr[i][j][0] = round_duration.mean
            arr[i][j][1] = round_duration.std
            arr[i][j][2] = validation_count.total_rate
            arr[i][j][3] = consensus_duration.mean
            arr[i][j][4] = consensus_duration.std

    x = (arr, facilitators, populations)

    with open(os.path.join(folder_name, 'pickle'), 'w') as f:
        pickle.dump(x, f)
    return x


def load_from_cache(folder_name):
    with open(os.path.join(folder_name, 'pickle'), 'r') as f:
        x = pickle.load(f)
    return x


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
    # p4 = plt.figure()
    # for i, populations in enumerate(populations):
    #     legend = '{}'.format(populations)
    #     plt.plot(facilitators, arr[:, i, Exp.consensus_duration_mean.value - 1], STYLES[i], label=legend, lw=LINE_WIDTH)


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
            if 'ACS: DONE':
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
