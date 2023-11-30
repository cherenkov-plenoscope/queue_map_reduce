import math
import os
import stat
import logging
import sys
import shutil
import time


def assign_tasks_to_chunks(num_tasks, num_chunks):
    """
    When you have too many tasks for your parallel processing queue this
    function chunks multiple tasks into fewer chunks.

    Parameters
    ----------
    num_tasks : int
        Number of tasks.
    num_chunks : int (optional)
        The maximum number of chunks. Your tasks will be spread over
        these many chunks. If None, each chunk contains a single task.

    Returns
    -------
        A list of chunks where each chunk is a list of task-indices `itask`.
        The lengths of the list of chunks is <= num_chunks.
    """
    if num_chunks is None:
        num_tasks_in_chunk = 1
    else:
        assert num_chunks > 0
        num_tasks_in_chunk = int(math.ceil(num_tasks / num_chunks))

    chunks = []
    current_chunk = []
    for j in range(num_tasks):
        if len(current_chunk) < num_tasks_in_chunk:
            current_chunk.append(j)
        else:
            chunks.append(current_chunk)
            current_chunk = []
            current_chunk.append(j)
    if len(current_chunk):
        chunks.append(current_chunk)
    return chunks


def make_path_executable(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def LoggerFile(path, name="file"):
    DATEFMT_ISO8601 = "%Y-%m-%dT%H:%M:%S"
    FMT = "{"
    FMT += '"t":"%(asctime)s.%(msecs)03d"'
    FMT += ", "
    FMT += '"c":"%(pathname)s:%(funcName)s:%(lineno)s"'
    FMT += ", "
    FMT += '"l":"%(levelname)s"'
    FMT += ", "
    FMT += '"m":"%(message)s"'
    FMT += "}"

    lggr = logging.Logger(name=name)
    file_handler = logging.FileHandler(filename=path, mode="w")
    fmtr = logging.Formatter(fmt=FMT, datefmt=DATEFMT_ISO8601)
    file_handler.setFormatter(fmtr)
    lggr.addHandler(file_handler)
    lggr.setLevel(logging.DEBUG)
    return lggr


def default_python_path():
    return os.path.abspath(shutil.which("python"))


def session_id_from_time_now():
    # This must be a valid filename. No ':' for time.
    return time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())


def make_JB_name_from_ichunk(session_id, ichunk):
    return "q{:s}#{:09d}".format(session_id, ichunk)


def make_ichunk_from_JB_name(JB_name):
    ichunk_str = JB_name.split("#")[1]
    return int(ichunk_str)
