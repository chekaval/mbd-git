import pickle
from math import floor

import matplotlib.pyplot as plt
import matplotlib.scale as scale

LIMIT_DAYS = False
MIN_DAYS = 0
MAX_DAYS = 365
LOG_SCALE = False

FILE_SUFFIX = 'most-popular-2023-01-25-13-25-39'

if __name__ == '__main__':
    with open("commit-trends-out/commit-trends-result-" + FILE_SUFFIX, "rb") as fp:
        result = pickle.load(fp)
    with open("commit-trends-out/commit-trends-users-count-" + FILE_SUFFIX, "rb") as fp:
        users_count = pickle.load(fp)

    days_since = [data[0] for data in result]
    avg_commits = [data[1]/users_count for data in result]

    # limit the days range
    if LIMIT_DAYS:
        from_idx = -1
        to_idx = -1
        for i in range(len(days_since)):
            data = days_since[i]
            # if data >= -365 and from_idx == -1:  # less than one year before account creation
            if data >= MIN_DAYS and from_idx == -1:
                from_idx = i  # set to the index of the first data in scope
            # elif data > 1825 and to_idx == -1:  # more than five years after account creation
            elif data > MAX_DAYS and to_idx == -1:
                to_idx = i
                break


    # measure for years

    # i = 0
    # days_since_bar = []
    # for val in days_since:
    #     if len(days_since_bar) <= i:
    #         days_since_bar.append(floor(val / 365))
    # for val in avg_commits
    # plt.bar()

    # plt.plot([data[0] for data in result], [float(data[1])/users_count for data in result])
    # days_since = [-1, 0, 1, 3, 4, 7, 8, 9, 14, 35]
    # avg_commits = [0.1, 0.6, 0.15, 0.05, 0.05, 0.1, 0.15, 1.9, 0.05, 0.15]
    if LIMIT_DAYS:
        plt.plot(days_since[from_idx:to_idx], avg_commits[from_idx:to_idx])
    else:
        plt.plot(days_since, avg_commits)

    if LOG_SCALE:
        plt.yscale('log')

    plt.title("Average Number of Daily User Commits Since Account Creation")
    plt.xlabel("Days Since Account Creation")
    plt.ylabel("Average # of Commits")
    plt.savefig("commit-trends-out/plot-popular-users-all.png")
