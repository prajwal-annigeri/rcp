import re
from collections import defaultdict

# File containing logs
log_file = "output/investigate/partition/logs_E.txt"

# Pattern to match heartbeat lines
pattern = re.compile(r"Finished heartbeat to (\w) after ([\d\.]+)(ms|µs)")

# Store total times and counts
totals = defaultdict(float)
counts = defaultdict(int)

lines = 0

with open(log_file, "r") as f:
    for line in f:
        if lines > 10000000:
            break

        match = pattern.search(line)
        if match:
            key, time_str, unit = match.groups()
            time = float(time_str)
            if unit == "µs":
                time /= 1000  # convert microseconds to milliseconds
            totals[key] += time
            counts[key] += 1

        lines += 1

# Compute averages
averages = {k: totals[k]/counts[k] for k in totals}
print(counts)

# Print results sorted by key
for k in sorted(averages):
    print(f"{k}: {averages[k]:.6f} ms")
