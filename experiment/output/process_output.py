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
        # print(f"Error: The file '{file_path}' was not found.")
        return [{"Count": "0", "50th(us)": "0"}] * 120
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return [{"Count": "0", "50th(us)": "0"}] * 120

def read_output_folder(folder_path):
    txt_files = [folder_path + "/" + f[:-4] for f in os.listdir(folder_path) if f.endswith('.txt')]
    results = []

    for file_name in txt_files:
        output = read_output_file(file_name + ".txt")
        throughput = []
        avg_latency = []
        median_latency = []

        for i in range(len(output)):
            throughput.append(int(output[i]["Count"]) if i == 0 else int(output[i]["Count"]) - int(output[i-1]["Count"]))
            avg_latency.append(int(output[i]["Avg(us)"]))
            median_latency.append(int(output[i]["50th(us)"]))

        file_attr_str = file_name.split("/")[-1]
        file_attr = {x.split("=")[0]: x.split("=")[1] for x in file_attr_str.strip().split("-")}
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

def plot_batch(data, duration, output_file_throughput="", output_file_latency=""):
    sorted_data = sorted(data, key=lambda x: int(x["client"]))

    batch_throughput_data = dict()
    batch_latency_data = dict()

    clients = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]

    for datum in sorted_data:
        batch_low, batch_high = datum["batch"].split(",")
        batch_low = int(batch_low)
        batch_high = int(batch_high)
        client = int(datum["client"])

        if client > 1024:
            continue

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

        throughput = avg(datum["throughput"][-duration:])
        latency = avg(datum["median_latency"][-duration:])

        if key not in batch_throughput_data:
            batch_throughput_data[key] = [float('nan')] * len(clients)
            batch_latency_data[key] = [float('nan')] * len(clients)

        batch_throughput_data[key][int(math.log2(client))] = throughput
        batch_latency_data[key][int(math.log2(client))] = latency
    
    # Show throughput data
    plt.figure()
    
    for label, data in batch_throughput_data.items():
        plt.plot(clients, data, marker='o', label=label)

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

    plt.xscale("log", base=2)
    plt.xlabel("Concurrent Clients")
    plt.ylabel("Median Latency (microseconds)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if output_file_latency != "":
        plt.savefig(output_file_latency)

    plt.show()

def plot_failure(data, failure, duration, tick=0, label_keys=["protocol"], output_file="", title="", smoothing=False):
    plt.figure()
    
    for datum in data:
        if datum["fail"] != failure:
            continue

        y = datum["throughput"][-duration:]
        
        if smoothing:
            y = smooth(y, window_size=5)

        label = ""
        for label_key in label_keys:
            label += label_key + "=" + datum[label_key] + " "
        label = label.strip()

        plt.plot(list(range(1, duration+1)), y, label=label)

    if tick != 0:
        plt.xticks(list(range(tick, duration, tick)))
    else:
        plt.xticks(list(range(duration // 6, duration, duration // 6)))

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

if __name__ == "__main__":
    FIELDS = ["Takes(s)", "Count", "OPS", "Avg(us)", "Min(us)", "Max(us)", "50th(us)", "90th(us)", "95th(us)", "99th(us)", "99.9th(us)", "99.99th(us)"]
    SMOOTH_DATA = False
    EXPERIMENT_TIME_BATCH = 30
    EXPERIMENT_TIME_NG = 30
    EXPERIMENT_TIME_G1 = 60
    EXPERIMENT_TIME_G2 = 240
    EXPERIMENT_TIME_K = 55
    EXPERIMENT_TIME_N = 110

    # ==================================================
    # Plot batch
    # ==================================================
    # G1: Client 512, Batch 256-512, BT=8
    # G2: Client 512, Batch 256-512, BT=30

    # NG_batch_folder = "output/batch_NG"
    # NG_batch_data = read_output_folder(NG_batch_folder)

    # plot_batch(NG_batch_data, EXPERIMENT_TIME_BATCH)

    # G1_batch_folder = "output/batch_G1"
    # G1_batch_data = read_output_folder(G1_batch_folder)

    # plot_batch(G1_batch_data, EXPERIMENT_TIME_BATCH)

    # G1_batch_folder = "output/batch_G2"
    # G1_batch_data = read_output_folder(G1_batch_folder)

    # plot_batch(G1_batch_data, EXPERIMENT_TIME_BATCH)

    # ==================================================
    # Plot NG Failures
    # ==================================================

    # NG_folder = "output/NG"
    # NG_data = read_output_folder(NG_folder)

    # plot_failure(NG_data, "None", EXPERIMENT_TIME_NG, title="None", smoothing=SMOOTH_DATA)
    # plot_failure(NG_data, "RF", EXPERIMENT_TIME_NG, title="RF", smoothing=SMOOTH_DATA)
    # plot_failure(NG_data, "LF", EXPERIMENT_TIME_NG, title="LF", smoothing=SMOOTH_DATA)
    # plot_failure(NG_data, "ROF", EXPERIMENT_TIME_NG, title="ROF", smoothing=SMOOTH_DATA)
    # plot_failure(NG_data, "LOF", EXPERIMENT_TIME_NG, title="LOF", smoothing=SMOOTH_DATA)

    # ==================================================
    # Plot G1 Failures
    # ==================================================

    # G1_folder = "output/G1"
    # G1_data = read_output_folder(G1_folder)

    # plot_failure(G1_data, "None", EXPERIMENT_TIME_G1, title="None", smoothing=SMOOTH_DATA)
    # plot_failure(G1_data, "RF", EXPERIMENT_TIME_G1, title="RF", smoothing=SMOOTH_DATA)
    # plot_failure(G1_data, "LF", EXPERIMENT_TIME_G1, title="LF", smoothing=SMOOTH_DATA)
    # # plot_failure(G1_data, "ROF", EXPERIMENT_TIME_G1, title="ROF", smoothing=SMOOTH_DATA)
    # # plot_failure(G1_data, "LOF", EXPERIMENT_TIME_G1, title="LOF", smoothing=SMOOTH_DATA)

    # ==================================================
    # Plot G2 Failures
    # ==================================================

    G2_folder = "output/G2"
    G2_data = read_output_folder(G2_folder)

    # plot_failure(G2_data, "None", EXPERIMENT_TIME_G2, title="None", smoothing=SMOOTH_DATA)
    plot_failure(G2_data, "RF", EXPERIMENT_TIME_G2, title="RF", smoothing=SMOOTH_DATA)
    # plot_failure(G2_data, "LF", EXPERIMENT_TIME_G2, title="LF", smoothing=SMOOTH_DATA)
    # plot_failure(G2_data, "ROF", EXPERIMENT_TIME_G2, title="ROF", smoothing=SMOOTH_DATA)
    # plot_failure(G2_data, "LOF", EXPERIMENT_TIME_G2, title="LOF", smoothing=SMOOTH_DATA)

    # ==================================================
    # Plot NG N
    # ==================================================

    # N_folder = "output/N"
    # N_data = read_output_folder(N_folder)

    # plot_failure(N_data, "None", EXPERIMENT_TIME_N, tick=5, label_keys=["protocol", "N"], title="None", smoothing=SMOOTH_DATA)
    # plot_failure(N_data, "RF", EXPERIMENT_TIME_N, tick=5, label_keys=["protocol", "N"], title="RF", smoothing=SMOOTH_DATA)
    # plot_failure(N_data, "LF", EXPERIMENT_TIME_N, tick=5, label_keys=["protocol", "N"], title="LF", smoothing=SMOOTH_DATA)
