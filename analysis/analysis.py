#!/usr/bin/env python3

import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import zipfile


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
    # plt.plot([1,2,3,4])
    # plt.ylabel('some numbers')
    # plt.show()
    extract_files(folder_name)

    for item in os.listdir(folder_name):
        if os.path.isdir(item):
            mean, std = _consensus_stats(item)
            # TODO


def _consensus_stats(folder_name):
    """
    Find the time difference between `TC: updated new promoters in round...`
    """
    for root, dirs, files in os.walk(folder_name):
        for d in dirs:
            print(d)
            for f in files:
                if ".err" in f:
                    print(os.path.join(root, f))
                    # TODO


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

