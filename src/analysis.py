#!/usr/bin/env python2

import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import zipfile
import dateutil


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


def plot_consensus(dirs):
    """

    :param dirs: 
    :return: 
    """
    def extract_data(folder_name):
        extract_files(folder_name)

        res = []
        for item in os.listdir(folder_name):
            full_path = os.path.join(folder_name, item)
            if os.path.isdir(full_path):
                mean, std = _consensus_stats(full_path)
                res.append([int(item), mean])

        res.sort(key=lambda x: x[0])
        res = np.transpose(res)
        return res

    for k, v in sorted(dirs.iteritems()):
        legend = v[0]
        style = v[1]
        res = extract_data(k)
        plt.plot(res[0], res[1], style, label=legend)

    plt.ylabel('Avg. time for one consensus round (seconds)')
    plt.xlabel('No. of facilitators (promoters)')
    plt.legend(loc='upper left')
    plt.grid()
    plt.show()


def plot_tx_rate(folder_name):
    pass


def _consensus_stats(folder_name):
    """
    Find the time difference between `TC: updated new promoters in round...`
    """
    fnames = []
    for root, dirs, files in os.walk(folder_name):
        for f in files:
            if ".err" in f:
                fnames.append(os.path.join(root, f))

    return _read_inverval_stats('TC: updated new promoters in round', fnames)


def _read_inverval_stats(match, fnames):
    differences = []

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


def _read_tx_rate(fname):
    """
    
    :param fname: 
    :return: (count, total_time)
    """
    match = 'TC: current tx count'
    lines_of_interest = find_lines_of_interest(match, fname)

    # take the start time as the first onee that does not have a 0 count
    start_t = None
    for line in lines_of_interest:
        count = int(line.split(match)[-1])
        if count != 0:
            start_t = datetime_from_line(line)
            break

    end_t = datetime_from_line(lines_of_interest[-1])
    end_count = int(lines_of_interest[-1].split(match)[-1])

    # return float(end_count) / difference_in_seconds(start_t, end_t)
    return end_count, difference_in_seconds(start_t, end_t)


def datetime_from_line(line):
    """
    log files are like this
    2017-04-09 21:57:02,001 - INFO ...
    we split ' - ' and take the 0th element as time
    :param line: 
    :return: 
    """
    return dateutil.parser.parse(line.split(' - ')[0])


def find_lines_of_interest(match, fname):
    """
    return lines that contain `match`
    :param match: 
    :param fname: 
    :return: 
    """
    lines_of_interest = []
    with open(fname, 'r') as f:
        for line in f:
            if match in line:
                lines_of_interest.append(line)

    return lines_of_interest


def difference_in_seconds(a, b):
    """
    a and b are both datetime type
    :param a: 
    :param b: 
    :return: 
    """
    assert b > a
    return (b - a).seconds


def expand_vars_in_key(s):
    return {os.path.expandvars(k): v for k, v in s.iteritems()}


if __name__ == '__main__':
    consensus_dirs = expand_vars_in_key({
        "$HOME/tudelft/consensus-experiment/consensus-500-5": ("1250 tx/s", 'x--'),
        "$HOME/tudelft/consensus-experiment/consensus-500-2": ("500 tx/s", 'o-'),
        "$HOME/tudelft/consensus-experiment/consensus-500-1": ("250 tx/s", 's-.'),
        "$HOME/tudelft/consensus-experiment/consensus-500-0": ("0 tx/s", '^:'),
        # "$HOME/tudelft/consensus-experiment/consensus-500-4-gossip": ("1000 tx/s (with gossip)", '+--'),
        "$HOME/tudelft/consensus-experiment/consensus-500-4-gossip2": ("1000 tx/s (with gossip)", '+--'),
        "$HOME/tudelft/consensus-experiment/consensus-500-2-gossip2": ("500 tx/s (with gossip)", '+--')
    })

    fns = {'consensus': plot_consensus}

    parser = argparse.ArgumentParser(description='Analyse experiment results.')
    parser.add_argument(
            '-p', '--plot',
            choices=fns.keys(),
            help='the type of experiment'
    )
    args = parser.parse_args()

    fns[args.plot](consensus_dirs)

