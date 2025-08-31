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

def plot_batch(data, output_file_throughput="", output_file_latency="", log=False):
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
            key = "Strat A, BT: " + datum["bt"]

        # Strategy B: batch low 1 * client, high 2 * client
        elif batch_low == client and batch_high == client * 2:
            key = "Strat B, BT: " + datum["bt"]

        # Strategy C: batch low 0.5 * client, high 1 * client
        elif batch_low == client // 2 and batch_high == client:
            key = "Strat C, BT: " + datum["bt"]

        # Strategy D: batch low 0.5 * client, high 2 * client
        elif batch_low == client // 2 and batch_high == client * 2:
            key = "Strat D, BT: " + datum["bt"]

        else:
            print(f"Strategy not found with client {client} and batch {batch_low}-{batch_high}")

        throughput = avg(datum["throughput"])
        latency = avg(datum["median_latency"])

        if key not in batch_throughput_data:
            batch_throughput_data[key] = [float('nan')] * len(clients)
            batch_latency_data[key] = [float('nan')] * len(clients)

        batch_throughput_data[key][int(math.log2(client))] = throughput
        batch_latency_data[key][int(math.log2(client))] = latency
    
    # Show throughput data
    plt.figure()
    
    for label, data in batch_throughput_data.items():
        plt.plot(clients, data, marker='o', label=label)

    if log:
        plt.xscale("log", base=2)
    plt.xlabel("Concurrent Clients")
    plt.ylabel("Average Throughput (transactions per second)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if output_file_throughput != "":
        plt.savefig(output_file_throughput)

    plt.show()
    
    # Show latency data
    plt.figure()
    
    for label, data in batch_latency_data.items():
        plt.plot(clients, data, marker='o', label=label)

    if log:
        plt.xscale("log", base=2)
    plt.xlabel("Concurrent Clients")
    plt.ylabel("Median Latency (microseconds)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if output_file_latency != "":
        plt.savefig(output_file_latency)

    plt.show()

def plot_failure(data, failure, tick=0, output_file="", title="", smoothing=False, color_K=False):
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

    plt.figure()
    
    for datum in data:
        if datum["fail"] != failure:
            continue

        y = datum["throughput"]
        
        if smoothing:
            y = smooth(y, window_size=5)

        label = datum["protocol"]
        if color_K:
            label += f"(K={datum['K']})"
        label += ": " + str(int(sum(y))) + " trxs"

        if color_K:
            plt.plot(list(range(1, len(y) + 1)), y,
                color=LINE_COLOR_MARKER_MAP[datum["K"]][0],
                marker=LINE_COLOR_MARKER_MAP[datum["K"]][1],
                linestyle=LINE_STYLE_MAP[datum["protocol"]],
                label=label)
        else:
            plt.plot(list(range(1, len(y) + 1)), y, linestyle=LINE_STYLE_MAP[datum["protocol"]], label=label)

    if tick == 0:
        tick = len(y) // 6

    plt.xticks(list(range(tick, len(y), tick)))

    plt.xlabel("Time (seconds)")
    plt.ylabel("Throughput (transactions per second)")
    plt.ylim(bottom=0)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if title != "":
        plt.title(title)

    if output_file != "":
        plt.savefig(output_file)

    plt.show()

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
    
    plot_batch(NG_batch_data, output_file_throughput="output/plots/batch_ng_throughput", output_file_latency="output/plots/batch_ng_latency")
    plot_batch(NG_batch_data, output_file_throughput="output/plots/batch_ng_throughput_log", output_file_latency="output/plots/batch_ng_latency_log", log=True)

    G1_batch_data = read_output_folders([
        "output/batch_G1/run1",
        "output/batch_G1/run2",
        "output/batch_G1/run3",
        ], EXPERIMENT_TIME_BATCH)
    
    plot_batch(G1_batch_data, output_file_throughput="output/plots/batch_g1_throughput", output_file_latency="output/plots/batch_g1_latency")
    plot_batch(G1_batch_data, output_file_throughput="output/plots/batch_g1_throughput_log", output_file_latency="output/plots/batch_g1_latency_log", log=True)

    G2_batch_data = read_output_folders([
        "output/batch_G2/run1",
        "output/batch_G2/run2",
        "output/batch_G2/run3",
        ], EXPERIMENT_TIME_BATCH)
    
    plot_batch(G2_batch_data, output_file_throughput="output/plots/batch_g2_throughput", output_file_latency="output/plots/batch_g2_latency")
    plot_batch(G2_batch_data, output_file_throughput="output/plots/batch_g2_throughput_log", output_file_latency="output/plots/batch_g2_latency_log", log=True)

    # ==================================================
    # Plot NG Failures
    # ==================================================

    NG_failure_data = read_output_folders([
        "output/failure_NG/run1",
        "output/failure_NG/run2",
        "output/failure_NG/run3",
        ], EXPERIMENT_TIME_NG)

    plot_failure(NG_failure_data, "None", title="NG None", output_file="output/plots/failure_ng_none", smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "RF", title="NG RF", output_file="output/plots/failure_ng_rf", smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "LF", title="NG LF", output_file="output/plots/failure_ng_lf", smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "ROF", title="NG ROF", output_file="output/plots/failure_ng_rof", smoothing=SMOOTH_DATA)
    plot_failure(NG_failure_data, "LOF", title="NG LOF", output_file="output/plots/failure_ng_lof", smoothing=SMOOTH_DATA)

    # ==================================================
    # Plot G1 Failures
    # ==================================================

    G1_failure_data = read_output_folders([
        "output/failure_G1/run1",
        "output/failure_G1/run2",
        "output/failure_G1/run3",
        ], EXPERIMENT_TIME_G1)

    plot_failure(G1_failure_data, "None", title="G1 None", output_file="output/plots/failure_g1_none", smoothing=SMOOTH_DATA)
    plot_failure(G1_failure_data, "LF", title="G1 LF", output_file="output/plots/failure_g1_lf", smoothing=SMOOTH_DATA)

    # ==================================================
    # Plot G2 Failures
    # ==================================================

    G2_failure_data = read_output_folders([
        "output/failure_G2/run1",
        "output/failure_G2/run2",
        "output/failure_G2/run3",
        ], EXPERIMENT_TIME_G2)

    plot_failure(G2_failure_data, "None", title="G2 None", output_file="output/plots/failure_g2_none", smoothing=SMOOTH_DATA)
    plot_failure(G2_failure_data, "LF", title="G2 LF", output_file="output/plots/failure_g2_lf", smoothing=SMOOTH_DATA)

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

    K_data = read_output_folders([
        "output/K/run1",
        "output/K/run2",
        "output/K/run3",
        ], EXPERIMENT_TIME_K)

    plot_failure(K_data, "None", tick=5, smoothing=SMOOTH_DATA, color_K=True, output_file="output/plots/k_no_failure")
    plot_failure(K_data, "RF", tick=5, smoothing=SMOOTH_DATA, color_K=True, output_file="output/plots/k_replica_failure")
    plot_failure(K_data, "LF", tick=5, smoothing=SMOOTH_DATA, color_K=True, output_file="output/plots/k_leader_failure")
