import sun_grid_engine_map as qmr
import numpy as np
import tempfile
import os
import pkg_resources
import json


def _tmp_path(name):
    return pkg_resources.resource_filename(
        "sun_grid_engine_map", os.path.join("tests", "resources", name)
    )


dummy_queue_state_path = _tmp_path("dummy_queue_state.json")
dummy_qsub_path = _tmp_path("dummy_qsub.py")
dummy_qstat_path = _tmp_path("dummy_qstat.py")
dummy_qdel_path = _tmp_path("dummy_qdel.py")


def test_dummys_exist():
    assert os.path.exists(dummy_qsub_path)
    assert os.path.exists(dummy_qstat_path)
    assert os.path.exists(dummy_qdel_path)


def init_dummy_queue_state(path, evil_jobs=[]):
    with open(path, "wt") as f:
        f.write(
            json.dumps({"running": [], "pending": [], "evil_jobs": evil_jobs})
        )


def test_run_with_failing_job():
    """
    The dummy_qsub will run the jobs.
    It will intentionally bring idx == 13 into error-state 'E' five times.
    This tests if qmr.map can recover this error using 10 trials.
    """
    with tempfile.TemporaryDirectory(prefix="sge") as tmp_dir:
        qsub_tmp_dir = os.path.join(tmp_dir, "qsub_tmp")

        init_dummy_queue_state(
            path=dummy_queue_state_path,
            evil_jobs=[{"idx": 13, "num_fails": 0, "max_num_fails": 5}],
        )

        NUM_JOBS = 30

        jobs = []
        for i in range(NUM_JOBS):
            job = np.arange(0, 100)
            jobs.append(job)

        results = qmr.map(
            function=np.sum,
            jobs=jobs,
            polling_interval_qstat=0.1,
            work_dir=qsub_tmp_dir,
            keep_work_dir=True,
            max_num_resubmissions=10,
            qsub_path=dummy_qsub_path,
            qstat_path=dummy_qstat_path,
            qdel_path=dummy_qdel_path,
            error_state_indicator="E",
        )

        for i in range(NUM_JOBS):
            assert results[i] == np.sum(jobs[i])
