import sys
import json

# Takes JSON output file(s) from FIO and outputs CSV including average thruput
#  and latency


def begin() -> bool:
    usage = f"Usage: {sys.argv[0]} <file1.json> [ <file2.json> ]..."
    if len(sys.argv) < 2:
        print(usage, file=sys.stderr)
        return False
    return True


def process_file(file: str):
    with open(file, "r") as f:
        json_data: dict = json.load(f)
    jobs: list = json_data["jobs"]
    if (len(jobs) < 1):
        print(f"Error: {file}: job data not found", file=sys.stderr)
        return
    job_name: str = jobs[0]["jobname"]
    print(f"{job_name}:", file=sys.stderr)
    thruput_kbps = "bw"
    latency = "lat_ns"
    read = "read"
    write = "write"
    trim = "trim"
    summary = {
        thruput_kbps: {
            read: 0,
            write: 0,
            trim: 0,
        },
        latency: {
            read: 0,
            write: 0,
            trim: 0,
        },
    }
    for job in jobs:
        for type in [read, write, trim]:
            io_data: dict = job[type]
            summary[thruput_kbps][type] += io_data[thruput_kbps]
            summary[latency][type] += io_data[latency]["mean"]
    print(f"Total jobs processed: {len(jobs)}", file=sys.stderr)
    print(f"IO Type,Average Thruput (KB/s),Average Latency (ms)")
    for type in [read, write, trim]:
        # convert from ns to ms, and average across all jobs
        summary[latency][type] /= (len(jobs) * 1000)
        print(f"{type},{summary[thruput_kbps][type]},{summary[latency][type]}")


if __name__ == "__main__":
    if not begin():
        exit(1)
    for file in sys.argv[1:]:
        process_file(file)
