def make_worker_node_script(func_module, func_name, environ):
    """
    Returns a string that is a python-script.
    This python-script will be executed on the worker-node.
    In here, the environment variables are set explicitly.
    It reads the chunk of tasks, runs result = func(task), and writes the
    results. The script is called on the worker-node with a single argument:

    python worker_node_script.py /path/to/work_dir/{ichunk:09d}.pkl

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
        "import os\n"
        "import sys\n"
        "import pickle\n"
        "import {func_module:s}\n"
        "from queue_map_reduce import network_file_system as nfs\n"
        "\n"
        "{add_environ:s}"
        "\n"
        "assert(len(sys.argv) == 2)\n"
        'chunk = pickle.loads(nfs.read(sys.argv[1], mode="rb"))\n'
        "task_results = []\n"
        "for j, task in enumerate(chunk):\n"
        "    try:\n"
        "        task_result = {func_module:s}.{func_name:s}(task)\n"
        "    except Exception as bad:\n"
        '        print("[task ", j, ", in chunk]", file=sys.stderr)\n'
        "        print(bad, file=sys.stderr)\n"
        "        task_result = None\n"
        "    task_results.append(task_result)\n"
        "\n"
        'nfs.write(pickle.dumps(task_results), sys.argv[1]+".out", mode="wb")\n'
        "".format(
            func_module=func_module,
            func_name=func_name,
            add_environ=add_environ,
        )
    )
