import pickle
import os
import time
import shutil
import json

from . import network_file_system as nfs
from . import queue
from . import utils


def chunk_path(work_dir, ichunk):
    return os.path.join(work_dir, "{:09d}.pkl".format(ichunk))


def has_invalid_or_non_empty_stderr(work_dir, num_chunks):
    has_errors = False
    for ichunk in range(num_chunks):
        e_path = chunk_path(work_dir, ichunk) + ".e"
        try:
            if os.stat(e_path).st_size != 0:
                has_errors = True
        except FileNotFoundError:
            has_errors = True
    return has_errors


def map_tasks_into_work_dir(work_dir, tasks, chunks, session_id):
    JB_names_in_session = []
    for ichunk, chunk in enumerate(chunks):
        JB_name = utils.make_JB_name_from_ichunk(
            session_id=session_id, ichunk=ichunk,
        )
        JB_names_in_session.append(JB_name)
        chunk_payload = [tasks[itask] for itask in chunk]
        nfs.write(
            content=pickle.dumps(chunk_payload),
            path=chunk_path(work_dir, ichunk),
            mode="wb",
        )
    return JB_names_in_session


def reduce_task_results_from_work_dir(work_dir, chunks, logger):
    task_results = []
    task_results_are_incomplete = False

    for ichunk, chunk in enumerate(chunks):
        num_tasks_in_chunk = len(chunk)
        chunk_result_path = chunk_path(work_dir, ichunk) + ".out"

        try:
            chunk_result = pickle.loads(
                nfs.read(path=chunk_result_path, mode="rb")
            )
            for task_result in chunk_result:
                task_results.append(task_result)
        except FileNotFoundError:
            task_results_are_incomplete = True
            logger.warning(
                "Expected results in: {:s}".format(chunk_result_path)
            )
            task_results += [None for i in range(num_tasks_in_chunk)]

    return task_results_are_incomplete, task_results


class Pool:
    """
    Multiprocessing on a compute-cluster using queues.
    """

    def __init__(
        self,
        queue_name=None,
        python_path=None,
        polling_interval_qstat=5,
        work_dir=None,
        keep_work_dir=False,
        max_num_resubmissions=10,
        error_state_indicator="E",
        logger=None,
        num_chunks=None,
        qsub_path="qsub",
        qstat_path="qstat",
        qdel_path="qdel",
    ):
        """
        Parameters
        ----------
        queue_name : string, optional
            Name of the queue.
        python_path : string, optional
            The python path to be used on the computing-cluster's worker-nodes
            to execute the worker-node's python-script.
        polling_interval_qstat : float, optional
            The time in seconds to wait before polling qstat again while
            waiting for the queue-jobs to finish.
        work_dir : string, optional
            The directory path where the tasks, the results and the
            worker-node-script is stored.
        keep_work_dir : bool, optional
            When True, the working directory will not be removed.
        max_num_resubmissions: int, optional
            In case of error-state in queue-job, the job will be tried this
            often to be resubmitted befor giving up on it.
        logger : logging.Logger(), optional
            Logger-instance from python's logging library. If None, a default
            logger is created which writes to sys.stdout.
        num_chunks : int, optional
            If provided, the tasks are grouped in this many chunks.
            The tasks in a chunk are computed in serial on the worker-node.
            It is useful to chunk tasks when the number of tasks is much larger
            than the number of available slots for parallel computing and the
            start-up-time for a slot is not much smaller than the compute-time
            for a single task.
        """

        self.queue_name = queue_name
        if python_path is None:
            self.python_path = utils.default_python_path()
        else:
            self.python_path = python_path
        self.polling_interval_qstat = polling_interval_qstat
        self.work_dir = work_dir
        self.keep_work_dir = keep_work_dir
        self.max_num_resubmissions = max_num_resubmissions
        self.error_state_indicator = error_state_indicator
        self.logger = logger
        self.num_chunks = num_chunks
        self.qsub_path = qsub_path
        self.qstat_path = qstat_path
        self.qdel_path = qdel_path

        if self.logger is None:
            self.logger = utils.LoggerStdout()

    def __repr__(self):
        return self.__class__.__name__ + "()"

    def map(self, func, iterable):
        """
        Apply `func` to each element in `iterable`, collecting the results
        in a list that is returned.

        Parameters
        ----------
        func : function-pointer
            Pointer to a function in a python-module. It must have both:
            func.__module__
            func.__name__
        iterable : list
            List of tasks. Each task must be a valid input to 'func'.

        Returns
        -------
        results : list
            Results. One result for each task.

        Example
        -------
        results = pool.map(sum, [[1, 2], [2, 3], [4, 5], ])
        """
        tasks = iterable
        session_id = utils.session_id_from_time_now()
        if self.work_dir is None:
            swd = os.path.abspath(os.path.join(".", ".qpool_" + session_id))
        else:
            swd = os.path.abspath(self.work_dir)
        sl = self.logger

        sl.debug("Starting map()")
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

        script_path = os.path.join(swd, "worker_node_script.py")
        sl.debug("Writing worker-node-script: {:s}".format(script_path))

        worker_node_script = queue.worker_node_script.make_worker_node_script(
            func_module=func.__module__,
            func_name=func.__name__,
            environ=dict(os.environ),
        )
        nfs.write(content=worker_node_script, path=script_path, mode="wt")
        utils.make_path_executable(path=script_path)

        sl.debug("Make chunks of tasks")

        chunks = utils.assign_tasks_to_chunks(
            num_tasks=len(tasks), num_chunks=self.num_chunks,
        )

        sl.info("Mapping chunks of tasks into work_dir")

        JB_names_in_session = map_tasks_into_work_dir(
            work_dir=swd, tasks=tasks, chunks=chunks, session_id=session_id,
        )

        sl.info("Submitting queue-jobs")

        for JB_name in JB_names_in_session:
            ichunk = utils.make_ichunk_from_JB_name(JB_name)
            queue.call.qsub(
                qsub_path=self.qsub_path,
                queue_name=self.queue_name,
                script_exe_path=self.python_path,
                script_path=script_path,
                arguments=[chunk_path(swd, ichunk)],
                JB_name=JB_name,
                stdout_path=chunk_path(swd, ichunk) + ".o",
                stderr_path=chunk_path(swd, ichunk) + ".e",
                logger=self.logger,
            )

        sl.info("Waiting for queue-jobs to finish")

        JB_names_in_session_set = set(JB_names_in_session)
        still_running = True
        num_resubmissions_by_ichunk = {}
        while still_running:
            all_jobs_running, all_jobs_pending = queue.call.qstat(
                qstat_path=self.qstat_path, logger=self.logger
            )

            (
                jobs_running,
                jobs_pending,
                jobs_error,
            ) = queue.job_organization.get_jobs_running_pending_error(
                JB_names_set=JB_names_in_session_set,
                error_state_indicator=self.error_state_indicator,
                all_jobs_running=all_jobs_running,
                all_jobs_pending=all_jobs_pending,
            )
            num_running = len(jobs_running)
            num_pending = len(jobs_pending)
            num_error = len(jobs_error)
            num_lost = 0
            for ichunk in num_resubmissions_by_ichunk:
                if (
                    num_resubmissions_by_ichunk[ichunk]
                    >= self.max_num_resubmissions
                ):
                    num_lost += 1

            sl.info(
                "{: 4d} running, {: 4d} pending, {: 4d} error, {: 4d} lost".format(
                    num_running, num_pending, num_error, num_lost,
                )
            )

            for job in jobs_error:
                ichunk = utils.make_ichunk_from_JB_name(job["JB_name"])
                if ichunk in num_resubmissions_by_ichunk:
                    num_resubmissions_by_ichunk[ichunk] += 1
                else:
                    num_resubmissions_by_ichunk[ichunk] = 1

                job_id_str = "JB_name {:s}, JB_job_number {:s}, ichunk {:09d}".format(
                    job["JB_name"], job["JB_job_number"], ichunk
                )
                sl.warning("Found error-state in: {:s}".format(job_id_str))
                sl.warning("Deleting: {:s}".format(job_id_str))

                queue.call.qdel(
                    JB_job_number=job["JB_job_number"],
                    qdel_path=self.qdel_path,
                    logger=self.logger,
                )

                if (
                    num_resubmissions_by_ichunk[ichunk]
                    <= self.max_num_resubmissions
                ):
                    sl.warning(
                        "Resubmitting {:d} of {:d}, JB_name {:s}".format(
                            num_resubmissions_by_ichunk[ichunk],
                            self.max_num_resubmissions,
                            job["JB_name"],
                        )
                    )
                    queue.call.qsub(
                        qsub_path=self.qsub_path,
                        queue_name=self.queue_name,
                        script_exe_path=self.python_path,
                        script_path=script_path,
                        arguments=[chunk_path(swd, ichunk)],
                        JB_name=job["JB_name"],
                        stdout_path=chunk_path(swd, ichunk) + ".o",
                        stderr_path=chunk_path(swd, ichunk) + ".e",
                        logger=self.logger,
                    )

            if jobs_error:
                nfs.write(
                    content=json.dumps(num_resubmissions_by_ichunk, indent=4),
                    path=os.path.join(swd, "num_resubmissions_by_ichunk.json"),
                    mode="wt",
                )

            if num_running == 0 and num_pending == 0:
                still_running = False

            time.sleep(self.polling_interval_qstat)

        sl.info("Reducing results from work_dir")
        (
            task_results_are_incomplete,
            task_results,
        ) = reduce_task_results_from_work_dir(
            work_dir=swd, chunks=chunks, logger=sl,
        )

        has_stderr = has_invalid_or_non_empty_stderr(
            work_dir=swd, num_chunks=len(chunks)
        )
        if has_stderr:
            sl.warning(
                "At least one task wrote to std-error or was not processed at all"
            )
        if has_stderr or self.keep_work_dir or task_results_are_incomplete:
            sl.warning("Keeping work_dir: {:s}".format(swd))
        else:
            sl.info("Removing work_dir: {:s}".format(swd))
            shutil.rmtree(swd)

        sl.debug("Stopping map()")

        return task_results
