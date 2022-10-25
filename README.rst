===========================
queue map reduce for python
===========================

|TravisBuildStatus| |PyPIStatus| |BlackStyle|

Queues for batch-jobs distribute your compute-tasks over multiple machines in parallel. This pool maps your tasks onto a queue and reduces the results.

.. code:: python

    import queue_map_reduce as qmr

    pool = qmr.Pool()
    results = pool.map(sum, [[1, 2], [2, 3], [4, 5], ])

A drop-in-replacement for builtins' ``map()``, and ``multiprocessing.Pool()``'s ``map()``.

Requirements
============

- Programs ``qsub``, ``qstat``, and ``qdel`` are required to submit, monitor, and delete queue-jobs.

- Your ``func(task)`` must be part of an importable python module.

- Your ``tasks`` and their ``results`` must be able to serialize using pickle.

- Both worker-nodes and process-node can read and write from and to a common path in the file-system.

Queue flavor
------------
Tested flavors are:

- Sun Grid Engine (SGE) 8.1.9

Features
========
- Respects fair-share, i.e. slots are only occupied when the compute is done.

- No spawning of additional threads. Neither on the process-node, nor on the worker-nodes.

- No need for databases or web-servers.

- Queue-jobs with error-state ``'E'`` can be deleted, and resubmitted until your predefined upper limit is reached.

- Can bundle tasks on worker-nodes to avoid start-up-overhead with many small tasks.

Alternatives
============
When you do not share resources with other users, and when you have some administrative power you might want to use one of these:

- Dask_ has a ``job_queue`` which also supports other flavors such as PBS, SLURM.

- pyABC.sge_ has a ``sge.map()`` very much like our one.

- ipyparallel_

Inner workings
==============
- ``map()`` makes a ``work_dir`` because the mapping and reducing takes place in the file-system. You can set ``work_dir`` manually to make sure both worker-nodes and process-node can reach it.

- ``map()`` serializes your ``tasks`` using ``pickle`` into separate files in ``work_dir/{ichunk:09d}.pkl``.

- ``map()`` reads all environment-variables in its process.

- ``map()`` creates the worker-node-script in ``work_dir/worker_node_script.py``. It contains and exports the process' environment-variables into the batch-job's context. It reads the chunk of tasks in ``work_dir/{ichunk:09d}.pkl``, imports and runs your ``func(task)``, and finally writes the result back to ``work_dir/{ichunk:09d}.pkl.out``.

- ``map()`` submits queue-jobs. The ``stdout`` and ``stderr`` of the tasks are written to ``work_dir/{ichunk:09d}.pkl.o`` and ``work_dir/{ichunk:09d}.pkl.e`` respectively. By default, ``shutil.which("python")`` is used to process the worker-node-script.

- When all queue-jobs are submitted, ``map()`` monitors their progress. In case a queue-job runs into an error-state (``'E'`` by default) the job wFill be deleted and resubmitted until a maximum number of resubmissions is reached.

- When no more queue-jobs are running or pending, ``map()`` will reduce the results from ``work_dir/{ichunk:09d}.pkl.out``.

- In case of non zero ``stderr`` in any task, a missing result, or on the user's request, the ``work_dir`` will be kept for inspection. Otherwise its removed.

Wording
-------

- ``task`` is a valid input to ``func``. The ``tasks`` are the actual payload to be processed.

- ``iterable`` is an iterable (list) of ``tasks``. It is the naming adopted from ``multiprocessing.Pool.map``.

- ``itask`` is the index of a ``task`` in ``iterable``.

- ``chunk`` is a chunk of ``tasks`` which is processed on a worker-node in serial.

- ``ichunk`` is the index of a chunk. It is used to create the chunks's filenames such as ``work_dir/{ichunk:09d}.pkl``.

- `queue-job` is what we submitt into the queue. Each queue-job processes the tasks in a single chunk in series.

- ``JB_job_number`` is assigned to a queue-job by the queue-system for its own book-keeping.

- ``JB_name`` is assigned to a queue-job by our ``map()``. It is composed of our ``map()``'s session-id, and ``ichunk``. E.g. ``"q"%Y-%m-%dT%H:%M:%S"#{ichunk:09d}"``

Environment Variables
---------------------
All the user's environment-variables in the process where ``map()`` is called will be exported in the queue-job's context.

The worker-node-script sets the environment-variables. We do not use ``qsub``'s argument ``-V`` because on some clusters this will not set all variables. Apparently some administrators fear security issues when using ``qsub -V`` to set ``LD_LIBRARY_PATH``.

Testing
=======

.. code:: bash

    py.test -s .

dummy queue
-----------
To test our ``map()`` we provide a dummy ``qsub``, ``qstat``, and ``qdel``.
These are individual ``python``-scripts which all act on a common state-file in ``tests/resources/dummy_queue_state.json`` in order to fake the sun-grid-engine's queue.

- ``dummy_qsub.py`` only appends queue-jobs to the list of pending jobs in the state-file.

- ``dummy_qdel.py`` only removes queue-jobs from the state-file.

- ``dummy_qstat.py`` does move the queue-jobs from the pending to the running list, and does trigger the actual processing of the jobs. Each time ``dummy_qstat.py`` is called it performs a single action on the state-file. So it must be called multiple times to process all jobs. It can intentionally bring jobs into the error-state when this is set in the state-file.

Before running the dummy-queue, its state-file must be initialized:

.. code:: python

    from queue_map_reduce import dummy_queue

    dummy_queue.init_queue_state(
        path="tests/resources/dummy_queue_state.json"
    )

When testing our ``map()`` you set its arguments ``qsub_path``, ``qdel_path``, and ``qstat_path`` to point to the dummy-queue.

See ``tests/test_full_chain_with_dummy_qsub.py``.

Because of the global state-file, only one instance of dummy_queue must run at a time.

.. |TravisBuildStatus| image:: https://travis-ci.org/cherenkov-plenoscope/queue_map_reduce.svg?branch=master
   :target: https://travis-ci.org/cherenkov-plenoscope/queue_map_reduce

.. |PyPIStatus| image:: https://badge.fury.io/py/queue_map_reduce_sebastian-achim-mueller.svg
   :target: https://pypi.org/project/queue-map-reduce-relleums

.. |BlackStyle| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black

.. _Dask: https://docs.dask.org/en/latest/

.. _pyABC.sge: https://pyabc.readthedocs.io/en/latest/api_sge.html

.. _ipyparallel: https://ipyparallel.readthedocs.io/en/latest/index.html
