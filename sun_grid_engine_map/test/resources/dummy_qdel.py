#!/usr/bin/env python3
import os
import sys
import json
import pkg_resources
import datetime

# dummy qdel

assert len(sys.argv) == 2
JB_job_number = sys.argv[1]

tmp_path = pkg_resources.resource_filename(
    "sun_grid_engine_map",
    os.path.join("test", "resources", "_tmp_qsub_state.json"),
)

with open(tmp_path, "rt") as f:
    old_state = json.loads(f.read())

found = False
state = {
    "pending": [],
    "running": [],
    "num_fails_of_evil_job": old_state["num_fails_of_evil_job"],
}
for job in old_state["running"]:
    if job["JB_job_number"] == JB_job_number:
        found = True
    else:
        state["running"].append(job)

for job in old_state["pending"]:
    if job["JB_job_number"] == JB_job_number:
        found = True
    else:
        state["pending"].append(job)

with open(tmp_path, "wt") as f:
    f.write(json.dumps(state, indent=4))

if found == True:
    sys.exit(0)
else:
    print("Can not find ", JB_job_number)
    sys.exit(1)
