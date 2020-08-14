import pickle
import os
import stat
import subprocess
import qstat
import time
import shutil
import tempfile


def _make_worker_node_script(module_name, function_name, environ):
    add_environ = ''
    for key in environ:
        add_environ += 'os.environ["{key:s}"] = "{value:s}"\n'.format(
            key=key,
            value=environ[key])

    return '' \
        '# Generated by sun_grid_engine_map.\n' \
        '# Do not modify.\n' \
        'from {module_name:s} import {function_name:s}\n' \
        'import pickle\n' \
        'import sys\n' \
        'import os\n' \
        '\n' \
        '{add_environ:s}' \
        '\n' \
        'assert(len(sys.argv) == 2)\n' \
        'with open(sys.argv[1], "rb") as f:\n' \
        '    job = pickle.loads(f.read())\n' \
        '\n' \
        'return_value = {function_name:s}(job)\n' \
        '\n' \
        'with open(sys.argv[1]+".out", "wb") as f:\n' \
        '    f.write(pickle.dumps(return_value))\n' \
        ''.format(
            module_name=module_name,
            function_name=function_name,
            add_environ=add_environ)


def _qsub(
    script_exe_path,
    script_path,
    arguments,
    job_name,
    stdout_path,
    stderr_path,
    queue_name=None,
):
    cmd = ['qsub']
    if queue_name:
        cmd += ['-q', queue_name]
    cmd += ['-o', stdout_path, ]
    cmd += ['-e', stderr_path, ]
    cmd += ['-N', job_name, ]
    cmd += ['-V', ]  # export enivronment variables to worker node
    cmd += ['-S', script_exe_path, ]
    cmd += [script_path]
    for argument in arguments:
        cmd += [argument]

    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print('returncode', e.returncode)
        print('output', e.output)
        raise


def _local_sub(
    script_exe_path,
    script_path,
    arguments,
    job_name,
    stdout_path,
    stderr_path,
    queue_name=None,
):
    cmd = [
        script_exe_path,
        script_path]
    for argument in arguments:
        cmd += [argument]
    with open(stdout_path, 'w') as fstdout:
        with open(stderr_path, 'w') as fstderr:
            subprocess.call(
                cmd,
                stdout=fstdout,
                stderr=fstderr)


def _job_path(work_dir, idx):
    return os.path.abspath(
        os.path.join(work_dir, "{:09d}.pkl".format(idx)))


def _timestamp():
    return time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime())


def _print(msg):
    print('{{"time": "{:s}", "msg": "{:s}"}}'.format(_timestamp(), msg,))


def _make_job_name(timestamp, idx):
    return "q{:s}.{:09d}".format(timestamp, idx)


def _has_non_zero_stderrs(work_dir, num_jobs):
    has_errors = False
    for idx in range(num_jobs):
        e_path = _job_path(work_dir, idx)+'.e'
        if os.stat(e_path).st_size != 0:
            has_errors = True
    return has_errors


def _num_jobs_running_and_pending(job_names_set):
    while True:
        try:
            running, pending = qstat.qstat()
            break
        except KeyboardInterrupt:
            raise
        except Exception as bad:
            print(bad)
            time.sleep(1)

    num_running = 0
    num_pending = 0
    for j in running:
        if j['JB_name'] in job_names_set:
            num_running += 1
    for j in pending:
        if j['JB_name'] in job_names_set:
            num_pending += 1
    return num_running, num_pending


def map(
    function,
    jobs,
    queue_name=None,
    python_path=os.path.abspath(shutil.which('python')),
    polling_interval_qstat=5,
    verbose=True,
    work_dir=None,
    keep_work_dir=False,
    additional_environment={},
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

    If qsub is not installed, map falls back to serial processing on the local
    machine. This allows testing on machines without qsub.

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
    verbose : bool, optional
        Print to stdout.
    work_dir : string, optional
        The directory path where the jobs, the results and the
        worker-node-script is stored.
    keep_work_dir : bool, optional
        When True, the working directory will not be removed.

    Example
    -------
    results = map(
        function=numpy.sum,
        jobs=[numpy.arange(i, 100+i) for i in range(10)])
    """
    timestamp = _timestamp()
    if work_dir is None:
        work_dir = os.path.abspath(os.path.join('.', '.qsub'+timestamp))

    QSUB = shutil.which('qsub') is not None

    if verbose:
        _print("Start map().")
        if QSUB:
            _print("Using qsub.")
        else:
            _print("No qsub. Falling back to serial.")

    os.makedirs(work_dir)
    if verbose:
        _print("Tmp dir {:s}".format(work_dir))

    if verbose:
        _print("Write jobs.")
    for idx, job in enumerate(jobs):
        with open(_job_path(work_dir, idx), 'wb') as f:
            f.write(pickle.dumps(job))

    if verbose:
        _print("Write worker node script.")

    script_str = _make_worker_node_script(
        module_name=function.__module__,
        function_name=function.__name__,
        environ=dict(os.environ))
    script_path = os.path.join(work_dir, 'worker_node_script.py')
    with open(script_path, "wt") as f:
        f.write(script_str)
    st = os.stat(script_path)
    os.chmod(script_path, st.st_mode | stat.S_IEXEC)

    if QSUB:
        submitt = _qsub
    else:
        submitt = _local_sub

    if verbose:
        _print("Submitt jobs.")

    job_names = []
    for idx in range(len(jobs)):
        job_names.append(_make_job_name(timestamp=timestamp, idx=idx))
        submitt(
            script_exe_path=python_path,
            script_path=script_path,
            arguments=[_job_path(work_dir, idx)],
            job_name=job_names[-1],
            queue_name=queue_name,
            stdout_path=_job_path(work_dir, idx)+'.o',
            stderr_path=_job_path(work_dir, idx)+'.e',)

    if verbose:
        _print("Wait for jobs to finish.")

    if QSUB:
        job_names_set = set(job_names)
        still_running = True
        while still_running:
            num_running, num_pending = _num_jobs_running_and_pending(
                job_names_set=job_names_set)
            if num_running == 0 and num_pending == 0:
                still_running = False
            if verbose:
                _print(
                    "{:d} running, {:d} pending".format(
                        num_running,
                        num_pending))
            time.sleep(polling_interval_qstat)

    if verbose:
        _print("Collect results.")

    results = []
    for idx, job in enumerate(jobs):
        try:
            result_path = _job_path(work_dir, idx)+'.out'
            with open(result_path, "rb") as f:
                result = pickle.loads(f.read())
            results.append(result)
        except FileNotFoundError:
            _print("ERROR. No result {:s}".format(result_path))
            results.append(None)

    if (
        _has_non_zero_stderrs(work_dir=work_dir, num_jobs=len(jobs)) or
        keep_work_dir
    ):
        _print("Found stderr.")
        _print("Keep work dir: {:s}".format(work_dir))
    else:
        shutil.rmtree(work_dir)

    if verbose:
        _print("Stop map().")

    return results
