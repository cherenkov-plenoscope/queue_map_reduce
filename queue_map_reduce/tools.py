import pickle
import os
import stat
import subprocess
import qstat
import time
import shutil
import json
import logging
import sys
from . import network_file_system as nfs


def _make_worker_node_script(module_name, function_name, environ):
    """
    Returns a string that is a python-script.
    This python-script will be executed on the worker-node.
    In here, the environment variables are set explicitly.
    It reads the job, runs result = function(job), and writes the result.
    The script will be called on the worker-node with a single argument:

    python script.py /some/path/to/work_dir/{idx:09d}.pkl

    On environment-variables
    ------------------------
    There is the '-V' option in qsub which is meant to export ALL environment-
    variables in the batch-job's context. And on many clusters this works fine.
    However, I encountered clusters where this does not work.
    For example ```LD_LIBRARY_PATH``` is often forced to be empty for reasons
    of security. So the admins say.
    This is why we set the einvironment-variables here in the
    worker-node-script.
    """
    add_environ = ""
    for key in environ:
        add_environ += 'os.environ["{key:s}"] = "{value:s}"\n'.format(
            key=key.encode("unicode_escape").decode(),
            value=environ[key].encode("unicode_escape").decode(),
        )

    return (
        ""
        "# I was generated automatically by queue_map_reduce.\n"
        "# I will be executed on the worker-nodes.\n"
        "# Do not modify me.\n"
        "from {module_name:s} import {function_name:s}\n"
        "import pickle\n"
        "import sys\n"
        "import os\n"
        "from queue_map_reduce import network_file_system as nfs\n"
        "\n"
        "{add_environ:s}"
        "\n"
        "assert(len(sys.argv) == 2)\n"
        'job = pickle.loads(nfs.read(sys.argv[1], mode="rb"))\n'
        "\n"
        "result = {function_name:s}(job)\n"
        "\n"
        'nfs.write(pickle.dumps(result), sys.argv[1]+".out", mode="wb")\n'
        "".format(
            module_name=module_name,
            function_name=function_name,
            add_environ=add_environ,
        )
    )


def _qsub(
    qsub_path,
    queue_name,
    script_exe_path,
    script_path,
    arguments,
    JB_name,
    stdout_path,
    stderr_path,
    logger,
):
    cmd = [qsub_path]
    if queue_name:
        cmd += ["-q", queue_name]
    cmd += ["-o", stdout_path]
    cmd += ["-e", stderr_path]
    cmd += ["-N", JB_name]
    cmd += ["-S", script_exe_path]
    cmd += [script_path]
    for argument in arguments:
        cmd += [argument]

    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.critical("Error in qsub()")
        logger.critical("qsub() returncode: {:d}".format(e.returncode))
        logger.critical(e.output)
        raise


def _job_path(work_dir, idx):
    return os.path.abspath(os.path.join(work_dir, "{:09d}.pkl".format(idx)))


def _session_id_from_time_now():
    # This must be a valid filename. No ':' for time.
    return time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())


def DefaultLoggerStdout():
    lggr = logging.Logger(name=__name__)
    fmtr = logging.Formatter(
        fmt="%(asctime)s, %(levelname)s, %(module)s:%(funcName)s, %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    stha = logging.StreamHandler(sys.stdout)
    stha.setFormatter(fmtr)
    lggr.addHandler(stha)
    lggr.setLevel(logging.DEBUG)
    return lggr


def _make_path_executable(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def _make_JB_name(session_id, idx):
    return "q{:s}#{:09d}".format(session_id, idx)


def _idx_from_JB_name(JB_name):
    idx_str = JB_name.split("#")[1]
    return int(idx_str)


def _has_invalid_or_non_empty_stderr(work_dir, num_jobs):
    has_errors = False
    for idx in range(num_jobs):
        e_path = _job_path(work_dir, idx) + ".e"
        try:
            if os.stat(e_path).st_size != 0:
                has_errors = True
        except FileNotFoundError:
            has_errors = True
    return has_errors


def __qdel(JB_job_number, qdel_path, logger):
    try:
        _ = subprocess.check_output(
            [qdel_path, str(JB_job_number)], stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as e:
        logger.critical("qdel returncode: {:s}".format(e.returncode))
        logger.critical("qdel stdout: {:s}".format(e.output))
        raise


def _qdel(JB_job_number, qdel_path, logger):
    while True:
        try:
            __qdel(JB_job_number, qdel_path, logger=logger)
            break
        except KeyboardInterrupt:
            raise
        except Exception as bad:
            logger.warning("Problem in qdel()")
            logger.warning(str(bad))
            time.sleep(1)


def _qstat(qstat_path, logger):
    """
    Return lists of running and pending jobs.
    Try again in case of Failure.
    Only except KeyboardInterrupt to stop.
    """
    while True:
        try:
            running, pending = qstat.qstat(qstat_path=qstat_path)
            break
        except KeyboardInterrupt:
            raise
        except Exception as bad:
            logger.warning("Problem in qstat()")
            logger.warning(str(bad))
            time.sleep(1)
    return running, pending


def _filter_jobs_by_JB_name(jobs, JB_names_set):
    my_jobs = []
    for job in jobs:
        if job["JB_name"] in JB_names_set:
            my_jobs.append(job)
    return my_jobs


def _extract_error_from_running_pending(
    jobs_running, jobs_pending, error_state_indicator
):
    # split into runnning, pending, and error
    _running = []
    _pending = []
    _error = []

    for job in jobs_running:
        if error_state_indicator in job["state"]:
            _error.append(job)
        else:
            _running.append(job)

    for job in jobs_pending:
        if error_state_indicator in job["state"]:
            _error.append(job)
        else:
            _pending.append(job)

    return _running, _pending, _error


def _jobs_running_pending_error(
    JB_names_set, error_state_indicator, qstat_path, logger
):
    all_jobs_running, all_jobs_pending = _qstat(
        qstat_path=qstat_path, logger=logger
    )
    jobs_running = _filter_jobs_by_JB_name(all_jobs_running, JB_names_set)
    jobs_pending = _filter_jobs_by_JB_name(all_jobs_pending, JB_names_set)
    return _extract_error_from_running_pending(
        jobs_running=jobs_running,
        jobs_pending=jobs_pending,
        error_state_indicator=error_state_indicator,
    )


def map_reduce(
    function,
    jobs,
    queue_name=None,
    python_path=os.path.abspath(shutil.which("python")),
    polling_interval_qstat=5,
    work_dir=None,
    keep_work_dir=False,
    max_num_resubmissions=10,
    qsub_path="qsub",
    qstat_path="qstat",
    qdel_path="qdel",
    error_state_indicator="E",
    logger=None,
):
    """
    Maps jobs to a function for embarrassingly parallel processing on a qsub
    computing-cluster.

    This for loop:

    >    results = []
    >    for job in jobs:
    >        results.append(function(job))

    will be executed in parallel on a qsub computing-cluster in order to obtain
    results.
    Both the jobs and results must be serializable using pickle.
    The function must be part of an installed python-module.

    Parameters
    ----------
    function : function-pointer
        Pointer to a function in a python module. It must have both:
        function.__module__
        function.__name__
    jobs : list
        List of jobs. A job in the list must be a valid input to function.
    queue_name : string, optional
        Name of the queue to submit jobs to.
    python_path : string, optional
        The python path to be used on the computing-cluster's worker-nodes to
        execute the worker-node's python-script.
    polling_interval_qstat : float, optional
        The time in seconds to wait before polling qstat again while waiting
        for the jobs to finish.
    work_dir : string, optional
        The directory path where the jobs, the results and the
        worker-node-script is stored.
    keep_work_dir : bool, optional
        When True, the working directory will not be removed.
    max_num_resubmissions: int, optional
        In case of error-state in job, the job will be tried this often to be
        resubmitted befor giving up on it.
    logger : logging.Logger(), optional
        Logger-instance from python's logging library. If None, a default
        logger is created which writes to sys.stdout.

    Example
    -------
    results = map(
        function=numpy.sum,
        jobs=[numpy.arange(i, 100+i) for i in range(10)]
    )
    """
    if logger is None:
        logger = DefaultLoggerStdout()

    session_id = _session_id_from_time_now()
    if work_dir is None:
        work_dir = os.path.abspath(os.path.join(".", ".qsub_" + session_id))

    logger.info("Starting map()")
    logger.debug("qsub_path: {:s}".format(qsub_path))
    logger.debug("qstat_path: {:s}".format(qstat_path))
    logger.debug("qdel_path: {:s}".format(qdel_path))
    logger.debug("queue_name: {:s}".format(str(queue_name)))
    logger.debug("python_path: {:s}".format(python_path))
    logger.debug(
        "polling-interval for qstat: {:f}s".format(polling_interval_qstat)
    )
    logger.debug("max. num. resubmissions: {:d}".format(max_num_resubmissions))
    logger.debug("error-state-indicator: {:s}".format(error_state_indicator))
    logger.info("Making work_dir {:s}".format(work_dir))
    os.makedirs(work_dir)

    script_path = os.path.join(work_dir, "worker_node_script.py")
    logger.debug("Writing worker-node-script: {:s}".format(script_path))
    worker_node_script_str = _make_worker_node_script(
        module_name=function.__module__,
        function_name=function.__name__,
        environ=dict(os.environ),
    )
    nfs.write(content=worker_node_script_str, path=script_path, mode="wt")
    _make_path_executable(path=script_path)

    logger.info("Mapping jobs into work_dir")
    JB_names_in_session = []
    for idx, job in enumerate(jobs):
        JB_name = _make_JB_name(session_id=session_id, idx=idx)
        JB_names_in_session.append(JB_name)
        nfs.write(
            content=pickle.dumps(job),
            path=_job_path(work_dir, idx),
            mode="wb",
        )

    logger.info("Submitting jobs")

    for JB_name in JB_names_in_session:
        idx = _idx_from_JB_name(JB_name)
        _qsub(
            qsub_path=qsub_path,
            queue_name=queue_name,
            script_exe_path=python_path,
            script_path=script_path,
            arguments=[_job_path(work_dir, idx)],
            JB_name=JB_name,
            stdout_path=_job_path(work_dir, idx) + ".o",
            stderr_path=_job_path(work_dir, idx) + ".e",
            logger=logger,
        )

    logger.info("Waiting for jobs to finish")

    JB_names_in_session_set = set(JB_names_in_session)
    still_running = True
    num_resubmissions_by_idx = {}
    while still_running:
        (
            jobs_running,
            jobs_pending,
            jobs_error,
        ) = _jobs_running_pending_error(
            JB_names_set=JB_names_in_session_set,
            error_state_indicator=error_state_indicator,
            qstat_path=qstat_path,
            logger=logger,
        )
        num_running = len(jobs_running)
        num_pending = len(jobs_pending)
        num_error = len(jobs_error)
        num_lost = 0
        for idx in num_resubmissions_by_idx:
            if num_resubmissions_by_idx[idx] >= max_num_resubmissions:
                num_lost += 1

        logger.info(
            "{: 4d} running, {: 4d} pending, {: 4d} error, {: 4d} lost".format(
                num_running, num_pending, num_error, num_lost,
            )
        )

        for job in jobs_error:
            idx = _idx_from_JB_name(job["JB_name"])
            if idx in num_resubmissions_by_idx:
                num_resubmissions_by_idx[idx] += 1
            else:
                num_resubmissions_by_idx[idx] = 1

            job_id_str = "JB_name {:s}, JB_job_number {:s}, idx {:09d}".format(
                job["JB_name"], job["JB_job_number"], idx
            )
            logger.warning("Found error-state in: {:s}".format(job_id_str))
            logger.warning("Deleting: {:s}".format(job_id_str))

            _qdel(
                JB_job_number=job["JB_job_number"],
                qdel_path=qdel_path,
                logger=logger,
            )

            if num_resubmissions_by_idx[idx] <= max_num_resubmissions:
                logger.warning(
                    "Resubmitting {:d} of {:d}, JB_name {:s}".format(
                        num_resubmissions_by_idx[idx],
                        max_num_resubmissions,
                        job["JB_name"],
                    )
                )
                _qsub(
                    qsub_path=qsub_path,
                    queue_name=queue_name,
                    script_exe_path=python_path,
                    script_path=script_path,
                    arguments=[_job_path(work_dir, idx)],
                    JB_name=job["JB_name"],
                    stdout_path=_job_path(work_dir, idx) + ".o",
                    stderr_path=_job_path(work_dir, idx) + ".e",
                    logger=logger,
                )

        if jobs_error:
            nfs.write(
                content=json.dumps(num_resubmissions_by_idx, indent=4),
                path=os.path.join(work_dir, "num_resubmissions_by_idx.json"),
                mode="wt",
            )

        if num_running == 0 and num_pending == 0:
            still_running = False

        time.sleep(polling_interval_qstat)

    logger.info("Reducing results from work_dir")

    results = []
    results_are_incomplete = False
    for idx, job in enumerate(jobs):
        try:
            result_path = _job_path(work_dir, idx) + ".out"
            result = pickle.loads(nfs.read(path=result_path, mode="rb"))
            results.append(result)
        except FileNotFoundError:
            results_are_incomplete = True
            logger.warning("No result: {:s}".format(result_path))
            results.append(None)

    has_stderr = False
    if _has_invalid_or_non_empty_stderr(work_dir=work_dir, num_jobs=len(jobs)):
        has_stderr = True
        logger.warning("Found non zero stderr")

    if has_stderr or keep_work_dir or results_are_incomplete:
        logger.warning("Keeping work_dir: {:s}".format(work_dir))
    else:
        logger.info("Removing work_dir: {:s}".format(work_dir))
        shutil.rmtree(work_dir)

    logger.info("Stopping map()")

    return results
