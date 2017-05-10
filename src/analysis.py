#!/usr/bin/env python2

import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import zipfile
import dateutil


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
            consensus_mean, consensus_std = _read_consensus_stats(full_path)
            validation_rate = _read_validation_rate(full_path)

            # NOTE below need to be in sync with `data_labels`
            arr[i][j][0] = consensus_mean
            arr[i][j][1] = consensus_std
            arr[i][j][2] = validation_rate

    return arr, facilitators, populations, data_labels


def plot(folder_name):
    """
    We have two "inputs" - population and no. of facilitator and two "outputs" - consensus time and throughput
    thus 4 graphs is needed. More inputs/outputs may be added later.
    :param folder_name: 
    :return: 
    """

    extract_files(folder_name)

    arr, facilitators, populations, data_labels = load_data(folder_name)
    print arr

    # plot throughput vs population
    p1 = plt.figure(1)
    throughtput_idx = 2
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

    # plot consensus vs population
    p2 = plt.figure(2)
    consensus_idx = 0
    assert data_labels[consensus_idx] == 'consensus mean'
    for i, facilitator in enumerate(facilitators):
        legend = '{} facilitators'.format(facilitator)
        plt.plot(populations, arr[i, :, consensus_idx], STYLES[i], label=legend)
    plt.ylabel('Consensus duration (s)')
    plt.xlabel('Population size')
    plt.legend(loc='upper left')
    plt.grid()
    p2.show()


def list_files_that_match(folder_name, match='.err'):
    fnames = []
    for root, dirs, files in os.walk(folder_name):
        for f in files:
            if match in f:
                fnames.append(os.path.join(root, f))
    return fnames


def _read_consensus_stats(folder_name):
    fnames = list_files_that_match(folder_name)
    differences = []
    match = 'updated new promoters'

    for fname in fnames:
        lines_of_interest = find_lines_of_interest(match, fname)

        times = [datetime_from_line(line) for line in lines_of_interest]
        idx = 0
        for a, b in zip(times, times[1:]):
            if "round {}".format(idx+1) in lines_of_interest[idx] \
                    and "round {}".format(idx+2) in lines_of_interest[idx+1]:
                differences.append(difference_in_seconds(a, b))
                idx += 1
            else:
                print "a round is skipped: round {} in file {}\n" \
                      "\t{}\n" \
                      "\t{}".format(idx, fname, lines_of_interest[idx+1], lines_of_interest[idx+2])
                break

    return np.mean(differences), np.std(differences)


def _read_validation_rate(folder_name):
    print "reading validation stats of {}".format(folder_name)
    fnames = list_files_that_match(folder_name)
    rates = []
    match = 'TC: verified'

    for fname in fnames:
        r = get_last_round(fname)
        lines_of_interest = find_lines_of_interest(match, fname, until='round {}, updated new promoters'.format(r))

        if len(lines_of_interest) == 0:
            print "WARNING: no lines of interest for {}".format(fname)
            continue
        start_t = datetime_from_line(lines_of_interest[0])
        end_t = datetime_from_line(lines_of_interest[-1])

        if not lines_of_interest:
            print "Nothing got validated for {}".format(fname)
            continue

        diff = difference_in_seconds(start_t, end_t)
        if diff == 0:
            print "Difference is zero for {}".format(fname)
            continue

        rate = float(len(lines_of_interest)) / diff
        rates.append(rate)

    return np.sum(rates)


def datetime_from_line(line):
    """
    log files are like this
    2017-04-09 21:57:02,001 - INFO ...
    we split ' - ' and take the 0th element as time
    :param line: 
    :return: 
    """
    return dateutil.parser.parse(line.split(' - ')[0])


def find_lines_of_interest(match, fname, until=None, ignore=None):
    """
    return lines that contain `match`
    :param match: 
    :param fname: 
    :param until: match a string for a cut-off point
    :param ignore: return empty list if there's a match
    :return: 
    """
    lines_of_interest = []
    with open(fname, 'r') as f:
        for line in f:
            if match in line:
                if ignore is not None and ignore in line:
                    return []
                if until is not None and until in line:
                    return lines_of_interest
                lines_of_interest.append(line)

    return lines_of_interest


def get_last_round(fname):
    match = 'updated new promoters'
    last = find_lines_of_interest(match, fname)[-1]
    return int(last.split('round ')[1][0])


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
    args = parser.parse_args()

    plot(os.path.expandvars(args.dir))

    raw_input()
