import sys
import json

def begin() -> bool:
    usage = f"Usage: {sys.argv[0]} <file1.json> [ <file2.json> ]..."
    if len(sys.argv) < 2:
        print(usage)
        return False
    return True

def process_file(file: str):
    # job_name: str = ".".join(file.split("/")[-1].split(".")[:-1])
    with open(file,"r") as f:
        json_data: dict = json.load(f)
    jobs: list = json_data["jobs"]
    if (len(jobs) < 1):
        print(f"Error: {file}: job data not found")
        return
    job_name: str = jobs[0]["jobname"]
    print(f"{job_name}:")
    total_bytes: str = "io_bytes"
    runtime: str = "job_runtime"
    latency: str = "lat_ns"
    read = "read"
    write = "write"
    trim = "trim"
    summary: dict = {
        total_bytes: {
            read: 0,
            write: 0,
            trim : 0,
        },
        latency: {
            read: 0,
            write: 0,
            trim: 0,
        },
        runtime: 0,
    }
    for job in jobs:
        summary[runtime] = job[runtime]
        for type in [ read, write, trim ]:
            io_data: dict = job[type]
            summary[total_bytes][type] += io_data["io_bytes"]
            summary[latency][type] = io_data[latency]["mean"]
            # avg_thruput: float = io_bytes * 1000 / runtime_ms
            # iops: int = io_data["iops"]
    

if __name__ == "__main__":
    if not begin():
        exit(1)
    for file in sys.argv[1:]:
        # job_name: str = ".".join(file.split("/")[-1].split(".")[:-1])
        process_file(file)