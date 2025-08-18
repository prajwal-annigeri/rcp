import pandas as pd
import matplotlib.pyplot as plt

def compare_runs(data_files: dict, plot_file_name_prefix: str):
    """
    Compare multiple CSV files with same format.
    Saves two plots: OPS vs Takes(s) and Avg(us) vs Takes(s)
    
    Args:
        data_files (dict): Keys are labels, values are CSV file paths
        plot_file_name_prefix (str): File path to save plot
    """

    WARMUP_TIME = 30

    # --- OPS Plot ---
    plt.figure()
    
    for label, file in data_files.items():
        df = pd.read_csv(file)
        df["Takes(s)"] = df["Takes(s)"].round().astype(int)

        # Filter and offset
        df = df[df["Takes(s)"] >= WARMUP_TIME].copy()
        df["TakesOffset"] = df["Takes(s)"] - WARMUP_TIME

        # Plot
        plt.plot(df["TakesOffset"], df["OPS"], marker='o', label=label)

    plt.xlabel("Time (seconds)")
    plt.ylabel("Throughput (transactions per second)")
    # plt.title("Throughput")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if plot_file_name_prefix:
        plt.savefig(plot_file_name_prefix + "_throughput.png")

    plt.show()

    # --- Avg(us) Plot ---
    plt.figure()

    for label, file in data_files.items():
        df = pd.read_csv(file)
        df["Takes(s)"] = df["Takes(s)"].round().astype(int)

        # Filter and offset
        df = df[df["Takes(s)"] >= WARMUP_TIME].copy()
        df["TakesOffset"] = df["Takes(s)"] - WARMUP_TIME

        # Plot
        plt.plot(df["TakesOffset"], df["Avg(us)"], marker='o', label=label)

    plt.xlabel("Time (seconds)")
    plt.ylabel("Average Latency (Microseconds)")
    # plt.title("Latency")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if plot_file_name_prefix:
        plt.savefig(plot_file_name_prefix + "_latency.png")

    plt.show()

if __name__ == "__main__":
    compare_runs(
        {
            "Baseline": "output/protocol=rcp-N=4-K=1-batch=512-client=512-fail=None.csv",
            "Optimized": "output/protocol=rcp-N=4-K=1-batch=512-client=512-fail=None.csv",
        },
        "",
    )
