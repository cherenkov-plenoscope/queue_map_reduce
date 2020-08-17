"""
A dummy queue for testing qsub, qstat, and qdel.
"""
import os
import pkg_resources
import json


def resource_path(name):
    return pkg_resources.resource_filename(
        "queue_map_reduce", os.path.join("tests", "resources", name)
    )


QUEUE_STATE_PATH = resource_path("dummy_queue_state.json")
QSUB_PATH = resource_path("dummy_qsub.py")
QSTAT_PATH = resource_path("dummy_qstat.py")
QDEL_PATH = resource_path("dummy_qdel.py")


def init_queue_state(path, evil_jobs=[]):
    with open(path, "wt") as f:
        f.write(
            json.dumps({"running": [], "pending": [], "evil_jobs": evil_jobs})
        )
