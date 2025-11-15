import math
import os

from matplotlib import pyplot as plt

def read_output_file(file_path):
    try:
        with open(file_path, 'r') as file:
            results = []
            lines = file.readlines()

            for line in lines:
                if line.startswith("TOTAL"):
                    results.append({x.split(": ")[0]: x.split(": ")[1] for x in line.strip().split("-")[1].strip().split(", ")})

                if len(line.strip()) == 0:
                    break

            return results

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def read_output_folders(folder_paths, duration):
    txt_files = [f for f in os.listdir(folder_paths[0]) if f.endswith('.txt')]
    txt_files.sort()

    for folder_path in folder_paths[1:]:
        txt_files_check = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
        txt_files_check.sort()

        if txt_files != txt_files_check:
            print("Folders don't have the same txt files")
            return None

    folder_count = len(folder_paths)
    results = []

    for txt_file in txt_files:
        throughput = [0] * duration
        avg_latency = [0] * duration
        median_latency = [0] * duration

        file_attr = {x.split("=")[0]: x.split("=")[1] for x in txt_file[:-4].strip().split("-")}

        for folder_path in folder_paths:
            output = read_output_file(folder_path + "/" + txt_file)
        
            for i in range(-duration, 0):
                throughput[i] = throughput[i] + ((int(output[i]["Count"]) - int(output[i-1]["Count"])) / folder_count)
                avg_latency[i] = avg_latency[i] + (int(output[i]["Avg(us)"]) / folder_count)
                median_latency[i] = median_latency[i] + (int(output[i]["50th(us)"]) / folder_count)

        file_attr["throughput"] = throughput
        file_attr["avg_latency"] = avg_latency
        file_attr["median_latency"] = median_latency

        results.append(file_attr)

    return results

def smooth(data, window_size=3):
    """
    Smooth data using a simple moving average.

    Args:
        data (list of float): Input data points.
        window_size (int): Number of points to average over.

    Returns:
        list of float: Smoothed data (same length as input).
    """
    if window_size < 1:
        raise ValueError("window_size must be >= 1")

    half_window = window_size // 2
    smoothed = []

    for i in range(len(data)):
        start = max(0, i - half_window)
        end = min(len(data), i + half_window + 1)
        smoothed.append(sum(data[start:end]) / (end - start))

    return smoothed

def avg(data):
    return sum(data) / len(data)

def median(data):
    sorted_data = sorted(data)
    n = len(data)
    mid = n // 2

    if n % 2 == 0:
        return (sorted_data[mid - 1] + sorted_data[mid]) / 2
    
    return sorted_data[mid]

def plot_batch(data, output_file="", log=False):
    sorted_data = sorted(data, key=lambda x: int(x["client"]))

    batch_throughput_data = dict()
    batch_latency_data = dict()

    clients = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]

    for datum in sorted_data:
        batch_low, batch_high = datum["batch"].split(",")
        batch_low = int(batch_low)
        batch_high = int(batch_high)
        client = int(datum["client"])

        # Strategy A: batch low 1 * client, high 1 * client
        if batch_low == client and batch_high == client:
            key = "A o"

        # Strategy B: batch low 1 * client, high 2 * client
        elif batch_low == client and batch_high == client * 2:
            key = "B s"

        # Strategy C: batch low 0.5 * client, high 1 * client
        elif batch_low == client // 2 and batch_high == client:
            key = "C ^"

        # Strategy D: batch low 0.5 * client, high 2 * client
        elif batch_low == client // 2 and batch_high == client * 2:
            key = "D d"

        else:
            print(f"Strategy not found with client {client} and batch {batch_low}-{batch_high}")

        throughput = avg(datum["throughput"])
        latency = median(datum["median_latency"])

        if key not in batch_throughput_data:
            batch_throughput_data[key] = [float('nan')] * len(clients)
            batch_latency_data[key] = [float('nan')] * len(clients)

        batch_throughput_data[key][int(math.log2(client))] = throughput
        batch_latency_data[key][int(math.log2(client))] = latency

    # Show throughput data
    _, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
    
    for key, data in batch_throughput_data.items():
        ax1.plot(clients, data, marker=key.split()[1], label=key.split()[0])

    if log:
        ax1.set_xscale("log", base=2)

    ax1.set_ylabel("Average Throughput (tps)")
    ax1.set_ylim(bottom=0)
    ax1.grid(True)
    ax1.legend(loc="lower right")
    
    # Show latency data
    for key, data in batch_latency_data.items():
        ax2.plot(clients, data, marker=key.split()[1], label=key.split()[0])

    if log:
        ax2.set_xscale("log", base=2)

    ax2.set_xlabel("Concurrent Clients")
    ax2.set_ylabel("Median Latency (ms)")
    ax2.set_ylim(bottom=0)
    ax2.grid(True)
    ax2.legend(loc="upper left")

    if output_file != "":
        plt.savefig(output_file)

    plt.show()

def plot_failure(data, failure, axes, tick=0, K="all", smoothing=False, color_K=False):
    LINE_STYLE_MAP = {
        "rcp": '-',
        "fraft": '--',
        "raft": ':',
    }

    LINE_COLOR_MARKER_MAP = {
        "1": ("red", "o"),
        "2": ("blue", "s"),
        "3": ("green", "^"),
        "4": ("orange", "d"),
        "5": ("purple", "*"),
    }
    
    for datum in data:
        if datum["fail"] != failure:
            continue

        if K != "all" and datum["K"] != K:
            continue

        y = datum["throughput"]
        
        if smoothing:
            y = smooth(y, window_size=5)

        label = datum["protocol"].capitalize()
        if label == "Fraft":
            label = "FRaft"
        if label == "Rcp":
            label = "Orca"

        if color_K:
            label += f"(K={datum['K']})"

        print(label, int(sum(y)))

        if color_K:
            axes.plot(list(range(1, len(y) + 1)), y,
                color=LINE_COLOR_MARKER_MAP[datum["K"]][0],
                marker=LINE_COLOR_MARKER_MAP[datum["K"]][1],
                linestyle=LINE_STYLE_MAP[datum["protocol"]],
                label=label)
        else:
            axes.plot(list(range(1, len(y) + 1)), y, linestyle=LINE_STYLE_MAP[datum["protocol"]], label=label)

    if tick == 0:
        tick = len(y) // 6

    axes.set_xticks(list(range(tick, len(y), tick)))

    # axes.set_ylabel("Throughput (tps)")
    axes.set_ylim(bottom=0)
    axes.grid(True)

def plot_N(data, failure, output_file="", title=""):
    N = [5, 11, 17, 23]
    protocols = ["rcp", "fraft", "raft"]

    avg_throughput = { protocol: [] for protocol in protocols }
    sorted_data = sorted(data, key=lambda x: int(x["N"]))

    for datum in sorted_data:
        if datum['fail'] != failure:
            continue

        throughput = avg(datum["throughput"])
        avg_throughput[datum['protocol']].append(throughput)

    x = range(len(N))
    width = 0.25

    _, ax = plt.subplots()
    print(avg_throughput)

    for i, p in enumerate(protocols):
        offsets = [pos + i * width for pos in x]
        ax.bar(offsets, avg_throughput[p], width, label=p)

    ax.set_xticks([pos + width for pos in x])  
    ax.set_xticklabels(N)

    ax.set_xlabel("N")
    ax.set_ylabel("Average Throughput")
    ax.legend()

    if title != "":
        plt.title(title)

    if output_file != "":
        plt.savefig(output_file)

    plt.show()

if __name__ == "__main__":
    FIELDS = ["Takes(s)", "Count", "OPS", "Avg(us)", "Min(us)", "Max(us)", "50th(us)", "90th(us)", "95th(us)", "99th(us)", "99.9th(us)", "99.99th(us)"]

    SMOOTH_DATA = False

    EXPERIMENT_TIME_BATCH = 30
    EXPERIMENT_TIME_NG = 30
    EXPERIMENT_TIME_G1 = 60
    EXPERIMENT_TIME_G2 = 240
    EXPERIMENT_TIME_K = 55
    EXPERIMENT_TIME_N = 60

    # ==================================================
    # Plot batch
    # ==================================================

    NG_batch_data = read_output_folders([
        "output/batch_NG/run1",
        "output/batch_NG/run2",
        "output/batch_NG/run3",
        ], EXPERIMENT_TIME_BATCH)
    
    plot_batch(NG_batch_data, output_file="output/plots/batch_ng")
    # plot_batch(NG_batch_data, output_file="output/plots/batch_ng_log", log=True)

    G1_batch_data = read_output_folders([
        "output/batch_G1/run1",
        "output/batch_G1/run2",
        "output/batch_G1/run3",
        ], EXPERIMENT_TIME_BATCH)
    
    plot_batch(G1_batch_data, output_file="output/plots/batch_g1")
    # plot_batch(G1_batch_data, output_file="output/plots/batch_g1_log", log=True)

    G2_batch_data = read_output_folders([
        "output/batch_G2/run1",
        "output/batch_G2/run2",
        "output/batch_G2/run3",
        ], EXPERIMENT_TIME_BATCH)
    
    plot_batch(G2_batch_data, output_file="output/plots/batch_g2")
    # plot_batch(G2_batch_data, output_file="output/plots/batch_g2_log", log=True)

    # ==================================================
    # Plot NG Failures
    # ==================================================

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

    NG_failure_data = read_output_folders([
        "output/failure_NG/run1",
        "output/failure_NG/run2",
        "output/failure_NG/run3",
        ], EXPERIMENT_TIME_NG)

    plot_failure(NG_failure_data, "None", ax1, smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "LF", ax2, smoothing=SMOOTH_DATA)

    ax2.set_xlabel("Timestamp (seconds)")
    handles, labels = ax1.get_legend_handles_labels()

    ax1.legend()
    ax2.legend()
    # fig.legend(handles, labels, loc="lower center", ncol=3)
    # plt.subplots_adjust(bottom=0.2)
    plt.savefig("output/plots/failure_ng")
    plt.show()


    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharey=True, sharex=True, figsize=(6.4, 5.6))
    
    plot_failure(NG_failure_data, "RF", ax1, smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "ROF", ax2, smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "LOF", ax3, smoothing=SMOOTH_DATA)

    ax3.set_xlabel("Timestamp (seconds)")
    handles, labels = ax1.get_legend_handles_labels()

    # ax1.legend(loc="lower left")
    # ax2.legend(loc="lower left")
    # ax3.legend(loc="lower left")
    fig.legend(handles, labels, loc="lower center", ncol=3)
    plt.subplots_adjust(bottom=0.2)
    plt.savefig("output/plots/failure_ng_others")
    plt.show()

    # ==================================================
    # Plot G1 Failures
    # ==================================================

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

    G1_failure_data = read_output_folders([
        "output/failure_G1/run1",
        "output/failure_G1/run2",
        "output/failure_G1/run3",
        ], EXPERIMENT_TIME_G1)

    plot_failure(G1_failure_data, "None", ax1, smoothing=SMOOTH_DATA)
    plot_failure(G1_failure_data, "LF", ax2, smoothing=SMOOTH_DATA)

    ax2.set_xlabel("Timestamp (seconds)")
    handles, labels = ax1.get_legend_handles_labels()

    ax1.legend()
    ax2.legend(loc="lower left")
    # fig.legend(handles, labels, loc="lower center", ncol=3)
    # plt.subplots_adjust(bottom=0.2)
    plt.savefig("output/plots/failure_g1")
    plt.show()

    # ==================================================
    # Plot G2 Failures
    # ==================================================

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

    G2_failure_data = read_output_folders([
        "output/failure_G2/run1",
        "output/failure_G2/run2",
        "output/failure_G2/run3",
        ], EXPERIMENT_TIME_G2)

    plot_failure(G2_failure_data, "None", ax1, smoothing=SMOOTH_DATA)
    plot_failure(G2_failure_data, "LF", ax2, smoothing=SMOOTH_DATA)

    ax2.set_xlabel("Timestamp (seconds)")
    handles, labels = ax1.get_legend_handles_labels()

    ax1.legend()
    ax2.legend(loc="lower left")
    # fig.legend(handles, labels, loc="lower center", ncol=3)
    # plt.subplots_adjust(bottom=0.2)
    plt.savefig("output/plots/failure_g2")
    plt.show()

    # ==================================================
    # Plot NG N
    # ==================================================

    N_data = read_output_folders([
        "output/N/run1",
        "output/N/run2",
        "output/N/run3",
        ], EXPERIMENT_TIME_N)

    plot_N(N_data, "None", title="N", output_file="output/plots/n_no_failure")

    # ==================================================
    # Plot NG K
    # ==================================================

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(6.4, 4.8))

    K_data = read_output_folders([
        "output/K/run1",
        "output/K/run2",
        "output/K/run3",
        ], EXPERIMENT_TIME_K)

    plot_failure(K_data, "None", ax1, tick=5, smoothing=SMOOTH_DATA, color_K=True)
    plot_failure(K_data, "RF", ax2, tick=5, smoothing=SMOOTH_DATA, color_K=True)

    ax2.set_xlabel("Timestamp (seconds)")
    handles, labels = ax1.get_legend_handles_labels()
    handles[1:-1] = [handles[5], handles[1], handles[6], handles[2], handles[7], handles[3], handles[8], handles[4]]
    labels[1:-1] = [labels[5], labels[1], labels[6], labels[2], labels[7], labels[3], labels[8], labels[4]]

    fig.legend(handles, labels, loc="lower center", ncol=5, prop={'size': 9})
    plt.subplots_adjust(bottom=0.28)
    plt.savefig("output/plots/k_others")
    plt.show()


    fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, sharey=True, sharex=True, figsize=(6.4, 7.2))

    plot_failure(K_data, "LF", ax1, K="1", tick=5, smoothing=SMOOTH_DATA, color_K=True)
    plot_failure(K_data, "LF", ax2, K="2", tick=5, smoothing=SMOOTH_DATA, color_K=True)
    plot_failure(K_data, "LF", ax3, K="3", tick=5, smoothing=SMOOTH_DATA, color_K=True)
    plot_failure(K_data, "LF", ax4, K="4", tick=5, smoothing=SMOOTH_DATA, color_K=True)
    plot_failure(K_data, "LF", ax5, K="5", tick=5, smoothing=SMOOTH_DATA, color_K=True)

    ax5.set_xlabel("Timestamp (seconds)")

    # fig.text(0.02, 0.55, 'Throughput (tps)', va='center', rotation='vertical')
    fig.legend(loc="lower center", ncol=5, prop={'size': 9})
    plt.subplots_adjust(top=0.92, bottom=0.18)
    plt.savefig("output/plots/k_leader_failure")
    plt.show()
