import queue_map_reduce as qmr
from queue_map_reduce import tools as qmr_tools
from queue_map_reduce import dummy_queue as dummy
import pickle
import numpy
import tempfile
import os
import subprocess


NUM_JOBS = 10
GOOD_FUNC = numpy.sum
GOOD_JOBS = []
for i in range(NUM_JOBS):
    work = numpy.arange(i, i + 100)
    GOOD_JOBS.append(work)
BAD_FUNC = os.path.join


def test_import():
    assert qmr.__file__ is not None


def test_make_worker_node_script():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        bundle = [numpy.arange(100)]
        func = numpy.sum
        with open(os.path.join(tmp, "bundle.pkl"), "wb") as f:
            f.write(pickle.dumps(bundle))
        s = qmr_tools._make_worker_node_script(
            func_module=func.__module__, func_name=func.__name__, environ={},
        )
        with open(os.path.join(tmp, "worker_node_script.py"), "wt") as f:
            f.write(s)
        rc = subprocess.call(
            [
                "python",
                os.path.join(tmp, "worker_node_script.py"),
                os.path.join(tmp, "bundle.pkl"),
            ]
        )
        assert rc == 0
        assert os.path.exists(os.path.join(tmp, "bundle.pkl") + ".out")
        with open(os.path.join(tmp, "bundle.pkl") + ".out", "rb") as f:
            result = pickle.loads(f.read())
        assert result == func(bundle[0])


def test_full_chain():
    func = GOOD_FUNC
    jobs = GOOD_JOBS

    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)
        pool = qmr.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
        )
        results = pool.map(func=func, jobs=jobs)

        assert len(results) == NUM_JOBS
        for i in range(NUM_JOBS):
            assert results[i] == func(jobs[i])


def test_force_dump_tmp_dir():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)
        pool = qmr.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            keep_work_dir=True,
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
        )
        results = pool.map(func=GOOD_FUNC, jobs=GOOD_JOBS,)
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_BAD_FUNC_creating_stderr():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)
        pool = qmr.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
        )
        results = pool.map(func=BAD_FUNC, jobs=GOOD_JOBS)
        assert len(results) == NUM_JOBS
        for r in results:
            assert r is None
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_one_bad_job_creating_stderr():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        bad_jobs = GOOD_JOBS.copy()
        bad_jobs.append("np.sum will not work for me.")

        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)
        pool = qmr.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
        )
        results = pool.map(func=GOOD_FUNC, jobs=bad_jobs,)

        assert len(results) == NUM_JOBS + 1
        for idx in range(NUM_JOBS):
            assert results[idx] == GOOD_FUNC(GOOD_JOBS[idx])
        assert results[idx + 1] is None
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_bundling_many_jobs():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)

        num_many_jobs = 120

        jobs = []
        for i in range(num_many_jobs):
            job = [i, i + 1, i + 2]
            jobs.append(job)

        pool = qmr.Pool(
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
            work_dir=os.path.join(tmp, "my_work_dir"),
            num_chunks=7,
        )
        results = pool.map(func=numpy.sum, jobs=jobs)

        assert len(results) == num_many_jobs
        for i in range(len(results)):
            assert results[i] == numpy.sum(jobs[i])
