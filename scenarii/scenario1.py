import json
import numpy as np
import requests
import threading
import time
import re
from collections import Counter

index_id = "test_index"


def create_indexes(url: str):
    print(f"Creating index {index_id}...")
    config = f"""
version: 0.8
index_id: {index_id}

doc_mapping:
  mode: dynamic
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats:
        - unix_timestamp
      output_format: unix_timestamp_secs
      fast: true
  timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 10
"""
    response = requests.post(
        f"{url}/api/v1/indexes",
        data=config,
        headers={"Content-Type": "application/yaml"},
    )
    if response.status_code != 200:
        raise Exception(
            f"Failed to create index {index_id} {response.status_code}: {response.text}"
        )
    print(f"Index {index_id} created")


class LogResults:
    def __init__(self):
        self.start_time = time.time()
        self.warn_count = 0
        self.error_count = 0
        self.slow_lock = 0
        self.shard_scale_up = []
        self.router_scale_up = []
        self.log_file = None

    def on_new_log_line(self, line: str):
        if "WARN" in line:
            self.warn_count += 1
        elif "ERROR" in line:
            self.error_count += 1
        if "lock acquisition took" in line:
            self.slow_lock += 1
        elif "successfully scaled up number of shards to" in line:
            # print(line)
            match = re.search(r"successfully scaled up number of shards to (\d+)", line)
            if match:
                nb_shard = int(match.group(1))
                self.shard_scale_up.append((nb_shard, time.time() - self.start_time))
            else:
                raise Exception(
                    f"Failed to parse number of shards after scale up: {line}"
                )

        elif "shards into routing table" in line:
            # print(line)
            match = re.search(r"inserted (\d+) shards into routing table", line)
            if match:
                shards_added = int(match.group(1))
                self.router_scale_up.append(
                    (shards_added, time.time() - self.start_time)
                )
            else:
                raise Exception(
                    f"Failed to parse number of shards added to router: {line}"
                )

    def shards_in_router(self) -> int:
        result = 1
        for shards_added, _ in self.router_scale_up:
            result += shards_added
        return result

    def print(self):
        if self.log_file is not None:
            print(f"Logs written to {self.log_file}")
        print(f"warn_count: {self.warn_count}")
        print(f"error_count: {self.error_count}")
        print(f"slow_lock: {self.slow_lock}")
        print(f"shard_scale_up: {self.shard_scale_up}")
        print(f"router_scale_up: {self.router_scale_up}")


def ingest_documents(url: str, log_results: LogResults):
    documents = [
        {
            "timestamp": int(time.time()),
            "id": "1",
            "content": "This is a test document.",
        },
        {"timestamp": int(time.time()), "id": "2", "content": "Another test document."},
    ] * 10000
    ndjson = "\n".join(json.dumps(doc) for doc in documents)
    start = time.time()
    ingest_rate_mibps = 40
    # run for 100 seconds
    ingest_total_mib = ingest_rate_mibps * 100
    target_request_interval = len(ndjson) / (ingest_rate_mibps * 1024 * 1024)
    status_codes = []
    request_durations = []
    # print("(status, time, mb)")
    for i in range(int(ingest_total_mib * 1024 * 1024 / len(ndjson))):
        # print(f"Ingesting {len(documents)} docs ({len(ndjson)} bytes) into index {index_id}...")
        def make_request(idx):
            req_start = time.time()
            response = requests.post(f"{url}/api/v1/{index_id}/ingest", data=ndjson)
            # print(
            #     (response.status_code, time.time() - start, len(ndjson) * idx / 1000000)
            # )
            status_codes.append((response.status_code, log_results.shards_in_router()))
            request_durations.append(time.time() - req_start)

        request_thread = threading.Thread(target=make_request, args=(i,))
        request_thread.start()
        time.sleep(target_request_interval)
    request_thread.join()

    print(f"Effective ingest rate: {ingest_total_mib/(time.time()-start):.2f} MiB/s")

    print(f"status codes: {Counter([code for code, _ in status_codes])}")
    print(
        f"status codes by shard count: {Counter([(code, shards) for code, shards in status_codes if code != 200])}"
    )
    print(f"request durations:")
    print(f"  p50: {np.percentile(request_durations, 50):.2f}s")
    print(f"  p90: {np.percentile(request_durations, 90):.2f}s")
    print(f"  p99: {np.percentile(request_durations, 99):.2f}s")
    print(f"  max: {np.max(request_durations):.2f}s")
