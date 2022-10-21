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
import math
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
        'bundle = pickle.loads(nfs.read(sys.argv[1], mode="rb"))\n'
        "job_results = []\n"
        "for j, job in enumerate(bundle):\n"
        "    try:\n"
        "        job_result = {function_name:s}(job)\n"
        "    except Exception as bad:\n"
        '        print("[job ", j, ", in bundle]", file=sys.stderr)\n'
        "        print(bad, file=sys.stderr)\n"
        "        job_result = None\n"
        "    job_results.append(job_result)\n"
        "\n"
        'nfs.write(pickle.dumps(job_results), sys.argv[1]+".out", mode="wb")\n'
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


def _bundle_path(work_dir, idx):
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


def _has_invalid_or_non_empty_stderr(work_dir, num_bundles):
    has_errors = False
    for idx in range(num_bundles):
        e_path = _bundle_path(work_dir, idx) + ".e"
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


def assign_jobs_to_bundles(num_jobs, num_bundles):
    """
    When you have too many jobs for your parallel processing queue this
    function bundles multiple jobs into fewer bundles.

    Parameters
    ----------
    num_jobs : int
        Number of jobs.
    num_bundles : int (optional)
        The maximum number of bundles. Your jobs will be spread over
        these many bundles. If None, each bundle contains a single job.

    Returns
    -------
        A list of bundles where each bundle is a list of job-indices.
        The lengths of the list of bundles is <= num_bundles.
    """
    if num_bundles is None:
        num_jobs_in_bundle = 1
    else:
        assert num_bundles > 0
        num_jobs_in_bundle = int(math.ceil(num_jobs / num_bundles))

    bundles = []
    current_bundle = []
    for j in range(num_jobs):
        if len(current_bundle) < num_jobs_in_bundle:
            current_bundle.append(j)
        else:
            bundles.append(current_bundle)
            current_bundle = []
            current_bundle.append(j)
    if len(current_bundle):
        bundles.append(current_bundle)
    return bundles


def _reduce_results(work_dir, bundles_of_jobs, logger):
    job_results = []
    job_results_are_incomplete = False

    for idx, bundle_of_jobs in enumerate(bundles_of_jobs):
        num_jobs_in_bundle = len(bundle_of_jobs)
        bundle_result_path = _bundle_path(work_dir, idx) + ".out"

        try:
            bundle_result = pickle.loads(
                nfs.read(path=bundle_result_path, mode="rb")
            )
            for job_result in bundle_result:
                job_results.append(job_result)
        except FileNotFoundError:
            job_results_are_incomplete = True
            logger.warning("No result: {:s}".format(bundle_result_path))
            job_results += [None for i in range(num_jobs_in_bundle)]

    return job_results_are_incomplete, job_results


class Pool:
    """
    Multiprocessing on a compute-cluster using queues.
    """

    def __init__(
        self,
        queue_name=None,
        python_path=os.path.abspath(shutil.which("python")),
        polling_interval_qstat=5,
        work_dir=None,
        keep_work_dir=False,
        max_num_resubmissions=10,
        error_state_indicator="E",
        logger=None,
        num_bundles=None,
        qsub_path="qsub",
        qstat_path="qstat",
        qdel_path="qdel",
    ):
        """
        Parameters
        ----------
        queue_name : string, optional
            Name of the queue to submit jobs to.
        python_path : string, optional
            The python path to be used on the computing-cluster's worker-nodes
            to execute the worker-node's python-script.
        polling_interval_qstat : float, optional
            The time in seconds to wait before polling qstat again while
            waiting for the jobs to finish.
        work_dir : string, optional
            The directory path where the jobs, the results and the
            worker-node-script is stored.
        keep_work_dir : bool, optional
            When True, the working directory will not be removed.
        max_num_resubmissions: int, optional
            In case of error-state in job, the job will be tried this often to
            be resubmitted befor giving up on it.
        logger : logging.Logger(), optional
            Logger-instance from python's logging library. If None, a default
            logger is created which writes to sys.stdout.
        num_bundles : int, optional
            If provided, the jobs will be grouped in this many bundles.
            The jobs in a bundle are computed serial on the worker-node.
            It is useful to bundle jobs when the number of jobs is much larger
            than the number of available slots for parallel computing and the
            start-up-time for a slot is not much smaller than the compute-time
            for a job.
        """

        self.queue_name = queue_name
        self.python_path = python_path
        self.polling_interval_qstat = polling_interval_qstat
        self.work_dir = work_dir
        self.keep_work_dir = keep_work_dir
        self.max_num_resubmissions = max_num_resubmissions
        self.error_state_indicator = error_state_indicator
        self.logger = logger
        self.num_bundles = num_bundles
        self.qsub_path = qsub_path
        self.qstat_path = qstat_path
        self.qdel_path = qdel_path

        if self.logger is None:
            self.logger = DefaultLoggerStdout()

    def __repr__(self):
        return self.__class__.__name__ + "()"

    def map(self, function, jobs):
        """
        Maps jobs to a function.
        Both jobs and results must be serializable using pickle.
        The function must be part of a python-module.

        Parameters
        ----------
        function : function-pointer
            Pointer to a function in a python-module. It must have both:
            function.__module__
            function.__name__
        jobs : list
            List of jobs. A job in the list must be a valid input to function.

        Returns
        -------
        results : list
            A list of the results. One result for each job.

        Example
        -------
        results = pool.map(sum, [[1, 2], [2, 3], [4, 5], ])
        """
        sl = self.logger
        swd = self.work_dir

        session_id = _session_id_from_time_now()

        if swd is None:
            swd = os.path.abspath(os.path.join(".", ".qsub_" + session_id))

        sl.info("Starting map()")
        sl.debug("qsub_path: {:s}".format(self.qsub_path))
        sl.debug("qstat_path: {:s}".format(self.qstat_path))
        sl.debug("qdel_path: {:s}".format(self.qdel_path))
        sl.debug("queue_name: {:s}".format(str(self.queue_name)))
        sl.debug("python_path: {:s}".format(self.python_path))
        sl.debug(
            "polling-interval for qstat: {:f}s".format(
                self.polling_interval_qstat
            )
        )
        sl.debug(
            "max. num. resubmissions: {:d}".format(self.max_num_resubmissions)
        )
        sl.debug(
            "error-state-indicator: {:s}".format(self.error_state_indicator)
        )

        sl.info("Making work_dir {:s}".format(swd))

        os.makedirs(swd)

        sl.debug("Writing worker-node-script: {:s}".format(script_path))

        script_path = os.path.join(swd, "worker_node_script.py")
        worker_node_script_str = _make_worker_node_script(
            module_name=function.__module__,
            function_name=function.__name__,
            environ=dict(os.environ),
        )
        nfs.write(content=worker_node_script_str, path=script_path, mode="wt")
        _make_path_executable(path=script_path)

        sl.info("Bundle jobs")

        bundles_of_jobs = assign_jobs_to_bundles(
            num_jobs=len(jobs), num_bundles=self.num_bundles,
        )

        sl.info("Mapping jobs into work_dir")

        JB_names_in_session = []
        for idx, bundle_of_jobs in enumerate(bundles_of_jobs):
            JB_name = _make_JB_name(session_id=session_id, idx=idx)
            JB_names_in_session.append(JB_name)
            bundle = [jobs[j] for j in bundle_of_jobs]
            nfs.write(
                content=pickle.dumps(bundle),
                path=_bundle_path(swd, idx),
                mode="wb",
            )

        sl.info("Submitting jobs")

        for JB_name in JB_names_in_session:
            idx = _idx_from_JB_name(JB_name)
            _qsub(
                qsub_path=self.qsub_path,
                queue_name=self.queue_name,
                script_exe_path=self.python_path,
                script_path=script_path,
                arguments=[_bundle_path(swd, idx)],
                JB_name=JB_name,
                stdout_path=_bundle_path(swd, idx) + ".o",
                stderr_path=_bundle_path(swd, idx) + ".e",
                logger=self.logger,
            )

        sl.info("Waiting for jobs to finish")

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
                error_state_indicator=self.error_state_indicator,
                qstat_path=self.qstat_path,
                logger=self.logger,
            )
            num_running = len(jobs_running)
            num_pending = len(jobs_pending)
            num_error = len(jobs_error)
            num_lost = 0
            for idx in num_resubmissions_by_idx:
                if num_resubmissions_by_idx[idx] >= self.max_num_resubmissions:
                    num_lost += 1

            sl.info(
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
                sl.warning("Found error-state in: {:s}".format(job_id_str))
                sl.warning("Deleting: {:s}".format(job_id_str))

                _qdel(
                    JB_job_number=job["JB_job_number"],
                    qdel_path=self.qdel_path,
                    logger=self.logger,
                )

                if num_resubmissions_by_idx[idx] <= self.max_num_resubmissions:
                    sl.warning(
                        "Resubmitting {:d} of {:d}, JB_name {:s}".format(
                            num_resubmissions_by_idx[idx],
                            self.max_num_resubmissions,
                            job["JB_name"],
                        )
                    )
                    _qsub(
                        qsub_path=self.qsub_path,
                        queue_name=self.queue_name,
                        script_exe_path=self.python_path,
                        script_path=script_path,
                        arguments=[_bundle_path(swd, idx)],
                        JB_name=job["JB_name"],
                        stdout_path=_bundle_path(swd, idx) + ".o",
                        stderr_path=_bundle_path(swd, idx) + ".e",
                        logger=self.logger,
                    )

            if jobs_error:
                nfs.write(
                    content=json.dumps(num_resubmissions_by_idx, indent=4),
                    path=os.path.join(swd, "num_resubmissions_by_idx.json"),
                    mode="wt",
                )

            if num_running == 0 and num_pending == 0:
                still_running = False

            time.sleep(self.polling_interval_qstat)

        sl.info("Reducing results from work_dir")
        job_results_are_incomplete, job_results = _reduce_results(
            work_dir=swd, bundles_of_jobs=bundles_of_jobs, logger=sl,
        )

        has_stderr = False
        if _has_invalid_or_non_empty_stderr(
            work_dir=swd, num_bundles=len(bundles_of_jobs)
        ):
            has_stderr = True
            sl.warning("Found non zero stderr")

        if has_stderr or self.keep_work_dir or job_results_are_incomplete:
            sl.warning("Keeping work_dir: {:s}".format(swd))
        else:
            sl.info("Removing work_dir: {:s}".format(swd))
            shutil.rmtree(swd)

        sl.info("Stopping map()")

        return job_results
