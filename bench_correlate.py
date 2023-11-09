import sys
import json

# Takes JSON output file(s) from FIO and outputs CSV including average thruput
#  and latency


def can_begin() -> bool:
    usage = f"Usage: {sys.argv[0]} <file1.json> [ <file2.json> ]..."
    if len(sys.argv) < 2:
        print(usage, file=sys.stderr)
        return False
    return True


def initialize():
    global read, write, trim, thruput_kbps, latency, metrics, io_types, aggr
    thruput_kbps = "bw"
    latency = "lat_ns"
    read = "read"
    write = "write"
    trim = "trim"
    io_types = [read, write, trim]
    metrics = {}
    aggr = {
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


def process_file(file: str):
    with open(file, "r") as f:
        json_data: dict = json.load(f)
    jobs: list = json_data["jobs"]
    if (len(jobs) < 1):
        print(f"Error: {file}: job data not found", file=sys.stderr)
        return
    job_name: str = jobs[0]["jobname"]
    print(f"{job_name}:", file=sys.stderr)
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
        for io_type in io_types:
            io_data: dict = job[io_type]
            summary[thruput_kbps][io_type] += io_data[thruput_kbps]
            summary[latency][io_type] += io_data[latency]["mean"]
    print(f"Total jobs processed: {len(jobs)}", file=sys.stderr)
    for io_type in io_types:
        # convert from ns to ms, and average across all jobs
        summary[latency][io_type] /= (len(jobs) * 1000)
    metrics[job_name] = summary


def aggregate():
    for type in io_types:
        aggr[thruput_kbps][type] = 0
        for job_name in metrics:
            aggr[thruput_kbps][type] += metrics[job_name][thruput_kbps][type]
            aggr[latency][type] += metrics[job_name][latency][type]
        aggr[latency][type] /= len(metrics)


def summarize():
    print(f"Job,Read Thruput (KB/s),Read Latency (ms),Write Thruput (KB/s),Write Latency (ms),Trim Thruput (KB/s),Trim Latency (ms)")
    for job_name in metrics:
        read_thruput = metrics[job_name][thruput_kbps][read]
        read_latency = metrics[job_name][latency][read]
        write_thruput = metrics[job_name][thruput_kbps][write]
        write_latency = metrics[job_name][latency][write]
        trim_thruput = metrics[job_name][thruput_kbps][trim]
        trim_latency = metrics[job_name][latency][trim]
        print(
            f"{job_name},{read_thruput},{read_latency:.0f},{write_thruput},{write_latency:.0f},{trim_thruput},{trim_latency:.0f}")
    read_thruput = aggr[thruput_kbps][read]
    read_latency = aggr[latency][read]
    write_thruput = aggr[thruput_kbps][write]
    write_latency = aggr[latency][write]
    trim_thruput = aggr[thruput_kbps][trim]
    trim_latency = aggr[latency][trim]
    print(
        f"Cumulative,{read_thruput},{read_latency:.0f},{write_thruput},{write_latency:.0f},{trim_thruput},{trim_latency:.0f}")


if __name__ == "__main__":
    if not can_begin():
        exit(1)
    initialize()
    for file in sys.argv[1:]:
        process_file(file)
    aggregate()
    summarize()
