import math


def calculate_latency_metrics(file_path: str):
    try:
        with open(file_path, "r") as f:
            # Parse non-empty lines as numbers
            latencies = [
                float(line.strip()) for line in f if line.strip()
            ]
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return
    except ValueError:
        print("Error: File contains non-numeric data.")
        return

    if not latencies:
        print("Error: File is empty.")
        return

    # Sort data for percentile calculations
    latencies.sort()
    n = len(latencies)

    # Helper function for percentile extraction (Nearest Rank method)
    def percentile(p):
        k = (p / 100.0) * (n - 1)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return latencies[int(k)]
        d0 = latencies[int(f)] * (c - k)
        d1 = latencies[int(c)] * (k - f)
        return d0 + d1

    # Key Metrics
    count = n
    min_lat = latencies[0]
    max_lat = latencies[-1]
    mean_lat = sum(latencies) / n
    variance = sum((x - mean_lat) ** 2 for x in latencies) / n
    std_dev = math.sqrt(variance)

    # Orderbook Percentiles
    p50 = percentile(50)  # Median
    p90 = percentile(90)
    p95 = percentile(95)
    p99 = percentile(99)
    p99_9 = percentile(99.9)  # HFT Tail Risk

    # Display Metrics
    print("=" * 45)
    print("       ORDERBOOK LATENCY ANALYSIS METRICS     ")
    print("=" * 45)
    print(f"Total Samples (Events) : {count:,}")
    print("-" * 45)
    print(f"Min Latency            : {min_lat:10.3f}")
    print(f"Mean Latency           : {mean_lat:10.3f}")
    print(f"Max Latency            : {max_lat:10.3f}")
    print(f"Std Deviation          : {std_dev:10.3f}")
    print("-" * 45)
    print("PERCENTILES & TAIL LATENCY:")
    print(f"  p50   (Median)       : {p50:10.3f}")
    print(f"  p90                  : {p90:10.3f}")
    print(f"  p95                  : {p95:10.3f}")
    print(f"  p99   (Tail)         : {p99:10.3f}")
    print(f"  p99.9 (Extreme Tail) : {p99_9:10.3f}")
    print("=" * 45)


# Example usage:
if __name__ == "__main__":
    # Replace 'latencies.txt' with your file path
    calculate_latency_metrics("latencies.txt")