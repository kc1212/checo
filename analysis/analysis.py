#!/usr/bin/env python3

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
        print("File already extracted, exception: {}".format(e))


def extract_files(folder_name):
    for root, dirs, files in os.walk(folder_name):
        for f in files:
            if f.split('.')[-1] == 'zip':
                full_path = os.path.join(root, f)
                print("trying to extract {}".format(full_path))
                extract_file(full_path)


def plot_consensus(folder_name):
    """
    Entry point for plotting the consensus graph.
    x-axis: no. of promoters
    y-axis: average time for one consensus round
    :param folder_name: 
    :return: 
    """
    extract_files(folder_name)

    res = []
    for item in os.listdir(folder_name):
        full_path = os.path.join(folder_name, item)
        if os.path.isdir(full_path):
            mean, std = _consensus_stats(full_path)
            res.append([int(item), mean])

    res.sort(key=lambda x: x[0])
    res = np.transpose(res)
    plt.plot(res[0], res[1], 'x--')
    plt.ylabel('Avg. time for one consensus round')
    plt.xlabel('No. of promoters')
    plt.grid()
    plt.show()


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
        lines_of_interest = []
        with open(fname, 'r') as f:
            for line in f:
                if match in line:
                    lines_of_interest.append(line)

        # log files are like this
        # 2017-04-09 21:57:02,001 - INFO ...
        # we split ' - ' and take the 0th element as time

        times = [dateutil.parser.parse(line.split(' - ')[0]) for line in lines_of_interest]
        for a, b in zip(times, times[1:]):
            differences.append(difference_in_seconds(a, b))

    return np.mean(differences), np.std(differences)


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
    fns = {'consensus': plot_consensus}

    parser = argparse.ArgumentParser(description='Analyse experiment results.')
    parser.add_argument('dir', help='the directory containing the log files')
    parser.add_argument(
            '-e', '--experiment',
            choices=fns.keys(),
            help='the type of experiment'
    )
    args = parser.parse_args()

    fns[args.experiment](args.dir)

