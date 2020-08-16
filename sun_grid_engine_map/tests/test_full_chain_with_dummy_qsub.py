import sun_grid_engine_map as qmr
from sun_grid_engine_map import _dummy_queue as dummy
import numpy as np
import tempfile
import os


def test_dummys_exist():
    assert os.path.exists(dummy.QSUB_PATH)
    assert os.path.exists(dummy.QSTAT_PATH)
    assert os.path.exists(dummy.QDEL_PATH)


def test_run_with_failing_job():
    """
    The dummy_qsub will run the jobs.
    It will intentionally bring idx == 13 into error-state 'E' five times.
    This tests if qmr.map can recover this error using 10 trials.
    """
    with tempfile.TemporaryDirectory(prefix="sge") as tmp_dir:
        qsub_tmp_dir = os.path.join(tmp_dir, "qsub_tmp")

        dummy.init_queue_state(
            path=dummy.QUEUE_STATE_PATH,
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
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
            error_state_indicator="E",
        )

        for i in range(NUM_JOBS):
            assert results[i] == np.sum(jobs[i])
