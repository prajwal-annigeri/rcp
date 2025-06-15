import numpy as np
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 3:
    print("Usage: python script.py file1.txt file2.txt")
    sys.exit(0)

filenames = sys.argv[1:3]

# Colors and labels for distinction
colors = ['blue', 'orange']
labels = [f'File: {filenames[0]}', f'File: {filenames[1]}']

plt.figure(figsize=(10, 6))

for i, filename in enumerate(filenames):
    # Read and process latencies
    with open(filename, 'r') as f:
        content = f.read().strip().rstrip(',')
        latencies = list(map(int, content.split(',')))
    
    latencies_sorted = np.sort(latencies)
    percentiles = np.linspace(0, 100, len(latencies_sorted), endpoint=True)

    # Plot each dataset
    plt.plot(latencies_sorted, percentiles, marker='o', label=labels[i], color=colors[i])

plt.title('Percentile vs Latency')
plt.ylabel('Percentile')
plt.xlabel('Latency (ms)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(f'percentile_vs_latency_comparison_{filenames[0]}_{filenames[1]}.png')
plt.show()
