import matplotlib.pyplot as plt
import numpy as np
from scipy.special import comb


CUSTOM_STYLES = ['--', '-', ':', '-.']


def pmf(M, N, n, i):
    """
    probability density function with notation from
    https://arxiv.org/pdf/1311.5939.pdf
    """
    return comb(M, i) * comb(N - M, n - i) / comb(N, n)

def sum_pmf(M, N, n, k):
    ks = np.arange(0, k+1, 1)
    return np.sum(pmf(M, N, n, ks))


if __name__ == '__main__':
    N = 2000
    ns = 3 * np.arange(1, 32, 10) + 1
    ts = np.floor((ns - 1) / 3)

    for (i, (n, t)) in enumerate(zip(ns, ts)):
        ms = np.arange(0, N, 1)

        pr = []
        for m in ms:
            pr.append(1 - sum_pmf(m, N, n, t))
        plt.plot(ms, pr, CUSTOM_STYLES[i], label="n = {:2}, t = {:2}".format(n, int(t)))
        plt.legend(loc='upper left')
        plt.xlabel('number of traitors')
        plt.ylabel(r'probability of electing more than $t = \lfloor \frac{n-1}{3} \rfloor$ traitors')
    plt.grid()
    plt.show()

