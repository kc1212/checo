#!/usr/bin/env python2

import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import zipfile
import dateutil
import pickle


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


LINE_STYLES = ['-', '--', '-.', ':']
MARKER_STYLES = ['x', '.', 'o', 's', '*', ',']
STYLES = [m + l for l in LINE_STYLES for m in MARKER_STYLES]


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
    data_labels = ['consensus mean', 'consensus std', 'throughput mean', 'throughput std']
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

    arr = np.empty([len(facilitators), len(populations), len(data_labels)])

    # iterate the directory again to actually read the data
    for i, facilitator in enumerate(facilitators):
        for j, population in enumerate(populations):
            full_path = os.path.join(folder_name, "{}-{}".format(facilitator, population))
            print "processing", full_path

            consensus = ConsensusReader()
            validation = ValidationReader()

            fnames = list_files_that_match(full_path)
            for fname in fnames:
                iter_line_with_cb(fname, [consensus.read_line, validation.read_line])
                consensus.finish_file(fname)
                validation.finish_file(fname)

            # NOTE below need to be in sync with `data_labels`
            arr[i][j][0] = consensus.mean
            arr[i][j][1] = consensus.std
            arr[i][j][2] = validation.total_rate

    x = (arr, facilitators, populations, data_labels)

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
        arr, facilitators, populations, data_labels = load_data(folder_name)
    else:
        try:
            arr, facilitators, populations, data_labels = load_from_cache(folder_name)
        except IOError:
            arr, facilitators, populations, data_labels = load_data(folder_name)

    consensus_idx = 0
    consensus_std_idx = 1
    throughtput_idx = 2

    # plot throughput vs population
    p1 = plt.figure(1)
    assert data_labels[throughtput_idx] == 'throughput mean'
    for i, facilitator in enumerate(facilitators):
        legend = '{} facilitators'.format(facilitator)
        # style =
        plt.plot(populations, arr[i, :, throughtput_idx], STYLES[i], label=legend)

    plt.ylabel('Throughput (validated tx / s)')
    plt.xlabel('Population size')
    plt.legend(loc='upper left')
    plt.grid()
    p1.show()

    # plot consensus duration vs population
    p2 = plt.figure(2)
    assert data_labels[consensus_idx] == 'consensus mean'
    for i, facilitator in enumerate(facilitators):
        legend = '{} facilitators'.format(facilitator)
        plt.plot(populations, arr[i, :, consensus_idx], STYLES[i], label=legend)
    plt.ylabel('Consensus duration (s)')
    plt.xlabel('Population size')
    plt.legend(loc='upper left')
    plt.grid()
    p2.show()

    # plot consensus vs facilitators
    p3 = plt.figure(3)
    for i, population in enumerate(populations):
        # TODO put 'population' in legend title
        legend = '{} population'.format(population)
        plt.plot(facilitators, arr[:, i, consensus_idx], STYLES[i], label=legend)
    plt.ylabel('Consensus duration (s)')
    plt.xlabel('Number of facilitators')
    plt.legend(loc='upper left')
    plt.grid()
    p3.show()


def list_files_that_match(folder_name, match='.err'):
    fnames = []
    for root, dirs, files in os.walk(folder_name):
        for f in files:
            if match in f:
                fnames.append(os.path.join(root, f))
    return fnames


class ConsensusReader(object):
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


class ValidationReader(object):
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
        default='$HOME/tudelft/consensus-experiment/new'
    )
    parser.add_argument(
        '--recompute',
        help='recompute, do not use the cache',
        action='store_true'
    )
    args = parser.parse_args()

    plot(os.path.expandvars(args.dir), args.recompute)

    raw_input()
