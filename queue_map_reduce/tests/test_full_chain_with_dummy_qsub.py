import queue_map_reduce as qmr
from queue_map_reduce import dummy_queue
import numpy as np
import tempfile
import os


def test_dummys_exist():
    assert os.path.exists(dummy_queue.QSUB_PATH)
    assert os.path.exists(dummy_queue.QSTAT_PATH)
    assert os.path.exists(dummy_queue.QDEL_PATH)


def test_run_with_failing_job():
    """
    The dummy_qsub will run the jobs.
    It will intentionally bring ichunk == 13 into error-state 'E' five times.
    This tests if qmr.map can recover this error using 10 trials.
    """
    with tempfile.TemporaryDirectory(prefix="sge") as tmp_dir:
        qsub_tmp_dir = os.path.join(tmp_dir, "qsub_tmp")

        dummy_queue.init_queue_state(
            path=dummy_queue.QUEUE_STATE_PATH,
            evil_jobs=[{"ichunk": 13, "num_fails": 0, "max_num_fails": 5}],
        )

        NUM_JOBS = 30

        jobs = []
        for i in range(NUM_JOBS):
            job = np.arange(0, 100)
            jobs.append(job)

        pool = qmr.Pool(
            polling_interval_qstat=0.1,
            work_dir=qsub_tmp_dir,
            keep_work_dir=True,
            max_num_resubmissions=10,
            qsub_path=dummy_queue.QSUB_PATH,
            qstat_path=dummy_queue.QSTAT_PATH,
            qdel_path=dummy_queue.QDEL_PATH,
            error_state_indicator="E",
        )

        results = pool.map(func=np.sum, jobs=jobs,)

        for i in range(NUM_JOBS):
            assert results[i] == np.sum(jobs[i])
