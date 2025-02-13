import json
import numpy as np
import requests
import threading
import time

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


def ingest_documents(url: str):
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
    ingest_total_mib = 2000
    target_request_interval = len(ndjson) / (ingest_rate_mibps * 1024 * 1024)
    status_codes = {200: 0, 429: 0}
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
            status_codes[response.status_code] += 1
            request_durations.append(time.time() - req_start)

        request_thread = threading.Thread(target=make_request, args=(i,))
        request_thread.start()
        time.sleep(target_request_interval)
    request_thread.join()

    print(f"Effective ingest rate: {ingest_total_mib/(time.time()-start):.2f} MiB/s")

    print(f"status codes: {status_codes}")
    print(f"request durations: {status_codes}")
    print(f"  p50: {np.percentile(request_durations, 50):.2f}s")
    print(f"  p90: {np.percentile(request_durations, 90):.2f}s")
    print(f"  p99: {np.percentile(request_durations, 99):.2f}s")
    print(f"  max: {np.max(request_durations):.2f}s")


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
        elif "successfully scaled up number of shards to " in line:
            # print(line)
            self.shard_scale_up.append(time.time() - self.start_time)
        elif "shards into routing table" in line:
            # print(line)
            self.router_scale_up.append(time.time() - self.start_time)

    def print(self):
        if self.log_file is not None:
            print(f"Logs written to {self.log_file}")
        print(f"warn_count: {self.warn_count}")
        print(f"error_count: {self.error_count}")
        print(f"slow_lock: {self.slow_lock}")
        print(f"shard_scale_up: {self.shard_scale_up}")
        print(f"router_scale_up: {self.router_scale_up}")
