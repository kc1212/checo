import matplotlib.pyplot as plt

plt.plot([1,2,3,4,4,4,4], lw=4)
plt.tick_params(
    axis='both',
    which='both',
    bottom='off',
    left='off',
    labelbottom='off',
    labelleft='off')
plt.ylabel('global throughput', fontsize=20)
plt.xlabel('population size $N$', fontsize=20)

plt.show()
