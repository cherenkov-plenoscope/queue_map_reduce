import queue_map_reduce as qmr
from queue_map_reduce import tools as qmr_tools
from queue_map_reduce import dummy_queue as dummy
import pickle
import numpy
import tempfile
import os
import subprocess


NUM_TASKS = 10
GOOD_FUNC = numpy.sum
GOOD_TASKS = []
for i in range(NUM_TASKS):
    work = numpy.arange(i, i + 100)
    GOOD_TASKS.append(work)
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
    tasks = GOOD_TASKS

    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)
        pool = qmr.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
        )
        results = pool.map(func=func, iterable=tasks)

        assert len(results) == NUM_TASKS
        for i in range(NUM_TASKS):
            assert results[i] == func(tasks[i])


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
        results = pool.map(func=GOOD_FUNC, iterable=GOOD_TASKS,)
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
        results = pool.map(func=BAD_FUNC, iterable=GOOD_TASKS)
        assert len(results) == NUM_TASKS
        for r in results:
            assert r is None
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_one_bad_task_creating_stderr():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        bad_tasks = GOOD_TASKS.copy()
        bad_tasks.append("np.sum will not work for me.")

        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)
        pool = qmr.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
        )
        results = pool.map(func=GOOD_FUNC, iterable=bad_tasks)

        assert len(results) == NUM_TASKS + 1
        for itask in range(NUM_TASKS):
            assert results[itask] == GOOD_FUNC(GOOD_TASKS[itask])
        assert results[itask + 1] is None
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_bundling_many_tasks():
    with tempfile.TemporaryDirectory(prefix="sge") as tmp:
        dummy.init_queue_state(path=dummy.QUEUE_STATE_PATH)

        num_many_tasks = 120

        tasks = []
        for i in range(num_many_tasks):
            task = [i, i + 1, i + 2]
            tasks.append(task)

        pool = qmr.Pool(
            polling_interval_qstat=1e-3,
            qsub_path=dummy.QSUB_PATH,
            qstat_path=dummy.QSTAT_PATH,
            qdel_path=dummy.QDEL_PATH,
            work_dir=os.path.join(tmp, "my_work_dir"),
            num_chunks=7,
        )
        results = pool.map(func=numpy.sum, iterable=tasks)

        assert len(results) == num_many_tasks
        for i in range(len(results)):
            assert results[i] == numpy.sum(tasks[i])
