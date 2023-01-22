
import matplotlib.pyplot as plt

if __name__ == '__main__':
    # plt.plot([data[0] for data in result], [float(data[1])/users_count for data in result])
    days_since = [-1, 0, 1, 3, 4, 7, 8, 9, 14, 35]
    avg_commits = [0.1, 0.6, 0.15, 0.05, 0.05, 0.1, 0.15, 1.9, 0.05, 0.15]
    plt.plot(days_since, avg_commits)

    # plt.title("Average Number of User Commits Relative to Account Creation Date")
    plt.title("Average Number of Daily User Commits Since Account Creation")
    plt.xlabel("Days Since Account Creation")
    plt.ylabel("Average # of Commits")
    plt.show()
