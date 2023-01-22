import pickle
import matplotlib.pyplot as plt

if __name__ == '__main__':
    with open("data/commit-trends-result", "rb") as fp:
        result = pickle.load(fp)
    with open("data/commit-trends-users-count", "rb") as fp:
        users_count = pickle.load(fp)

    days_since = [data[0] for data in result]
    avg_commits = [float(data[1])/users_count for data in result]
    # plt.plot([data[0] for data in result], [float(data[1])/users_count for data in result])
    # days_since = [-1, 0, 1, 3, 4, 7, 8, 9, 14, 35]
    # avg_commits = [0.1, 0.6, 0.15, 0.05, 0.05, 0.1, 0.15, 1.9, 0.05, 0.15]
    plt.plot(days_since, avg_commits)
    # plt.xscale
    #TODO find out commits that are older than 1970..??? ~50+ years -- search for max dates (array_max) in commits / min date in user creation...
    # TODO filter out future commits (anything after the dataset was created?!)

    # plt.title("Average Number of User Commits Relative to Account Creation Date")
    plt.title("Average Number of Daily User Commits Since Account Creation")
    plt.xlabel("Days Since Account Creation")
    plt.ylabel("Average # of Commits")
    plt.show()
