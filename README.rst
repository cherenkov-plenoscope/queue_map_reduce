===========================
queue map reduce for python
===========================

|TravisBuildStatus| |PyPIStatus| |BlackStyle|

Queues for batch-jobs are a powerful tool to distribute your compute-jobs over multiple machines in parallel. This single function maps your jobs onto a queue, and reduces the results.

.. code:: python

    import queue_map_reduce as qmr
    import numpy

    results = qmr.map(
        function=numpy.sum,
        jobs=[numpy.arange(i, 100+i) for i in range(10)]
    )

A drop-in-replacement for the standard's ``map()``, and ``multiprocessing.Pool()``'s ``map()``.

Requirements
============

- Programs ``qsub``, ``qstat``, and ``qdel`` are required to submit, monitor, and delete jobs.

- Your ``function(job)`` must be part of an importable python module.

- Your ``jobs`` and their ``results`` must be able to serialize using pickle.

- All worker-nodes and the process-node itself running the ``map()`` must share a common path in the file-system were they can read and write.

Queue flavor
------------
Different flavors of ``qsub``, ``qstat``, and ``qdel`` might have different interfaces. Tested flavors are:

- Sun Grid Engine (SGE) 8.1.9

Features
========
- Only a single, stateless function ``map()``.

- Jobs with error-state ``'E'`` can be deleted, and resubmitted until your predefined upper limit is reached.

- Respecting fair-share, i.e. slots are only occupied when they run your jobs.

- No spawning of additional threads. Neither on the process-node, nor on the worker-nodes.

- No need for databases or web-servers.

Inner workings
==============
- Our ``map()`` makes a ``work_dir`` because the mapping and reduction takes place in the file-system. You can set ``work_dir`` manually to ensure it is in a common path of all worker-nodes and the process-node.

- Our ``map()`` serializes each of your ``jobs`` using ``pickle`` into a separate file in ``work_dir/{idx:09d}.pkl``.

- Our ``map()`` reads all environment-variables in its process.

- Our ``map()`` creates the worker-node-script in ``work_dir/worker_node_script.py``. It contains and exports the process' environment-variables into the batch-job's context. It reads the job in ``work_dir/{idx:09d}.pkl``, imports and runs your ``function(job)``, and finally writes the result back to ``work_dir/{idx:09d}.pkl.out``.

- Our ``map()`` submits your jobs into the queue. The ``stdout`` and ``stderr`` of the jobs are written to ``work_dir/{idx:09d}.pkl.o`` and ``work_dir/{idx:09d}.pkl.e`` respectively. By default, ``shutil.which("python")`` is used to process the worker-node-script.

- When all jobs are submitted, ``map()`` monitors the progress of its jobs. In case a job will run into an error-state, which is ``'E'`` by default, the job will be deleted and resubmitted until a maximum number of resubmissions is reached.

- When no more jobs are running or pending, ``map()`` will reduce the results. It will read each result from ``work_dir/{idx:09d}.pkl.out`` and append it to the list of results.

- In case of non zero ``stderr`` in any job, a missing result, or on the user's request, the ``work_dir`` will be kept for inspection. Otherwise its removed.


Identifying jobs
----------------
- ``JB_job_number`` is assigned to your job by the queue-system for its own book-keeping.

- ``JB_name`` is the name assigned to your job by our ``map()``. It is composed of the ``map()``'s session-name, and the ``idx`` of your job with respect to your lists of jobs. E.g. ``"q"%Y-%m-%dT%H:%M:%S"#{idx:09d}"``

- ``idx`` is only used within our ``map()``. It is the index of your job with respect to your list of jobs. It is written into ``JB_name``, and it is the ``idx`` used to create the job's filenames such as ``work_dir/{idx:09d}.pkl``.


Environment Variables
---------------------
All the user's environment-variables in the process where ``queue_map_reduce.map()`` is called will be exported in the job's context.

The worker-node-script sets the environment-variables before calling ``function(job)``. We do not use ``qsub``'s argument ``-V`` because on some clusters this will not set all variables. Apparently some administrators fear security issues when using ``qsub -V`` to set ``LD_LIBRARY_PATH``.

Testing
=======

.. code:: bash

    py.test -s .


dummy queue
-----------
To test our ``map()`` we provide a dummy ``qsub``, ``qstat``, and ``qdel``.
These are individual ``python``-scripts which all act on a common state-file in ``tests/resources/dummy_queue_state.json`` in order to fake the sun-grid-engine's queue.

- ``dummy_qsub.py`` only appends jobs to the list of pending jobs in the state-file.

- ``dummy_qdel.py`` only removes jobs from the state-file.

- ``dummy_qstat.py`` does move the jobs from the pending to the running list, and does trigger the actual processing of the jobs. Each time ``dummy_qstat.py`` is called it performs a single action on the state-file. So it must be called multiple times to process all jobs. It can intentionally bring jobs into the error-state when this is set in the state-file.

Before running the dummy-queue, its state-file must be initialized:

.. code:: python

    from queue_map_reduce import dummy_queue

    dummy_queue.init_queue_state(
        path="tests/resources/dummy_queue_state.json"
    )

When testing our ``map()`` you set its arguments ``qsub_path``, ``qdel_path``, and ``qstat_path`` to point to the dummy-queue.

See ``tests/test_full_chain_with_dummy_qsub.py``.

Because of the global state-file, only one instance of dummy_queue must run at a time.

Scope
=====
Our scope is intentionally limited to embarrassingly simple parallel computing with a ``map()`` function while

we assume the user
------------------
- has to fair-share the queue with other users **all** the time.

- can not login to worker-nodes, thus can not communicate with her jobs when these are running.

- can only write to her own ``$HOME``.

- has to build and install any dependency from source in her own ``$HOME`` with an old ``gcc``.

- has a high error-rate because of jobs dying in resource-conflicts with other users.

Alternatives
============
When you do not share resources with other users, and when you have some administrative power than have a look at these:

- `Dask<https://docs.dask.org/en/latest/>` has a ``job_queue`` which also supports other flavors such as PBS, SLURM.

- `pyABC.sge<https://pyabc.readthedocs.io/en/latest/api_sge.html>` has a ``sge.map()`` very much like our one.

- `ipyparallel<https://ipyparallel.readthedocs.io/en/latest/index.html>`


.. |TravisBuildStatus| image:: https://travis-ci.org/cherenkov-plenoscope/queue_map_reduce.svg?branch=master
   :target: https://travis-ci.org/cherenkov-plenoscope/queue_map_reduce

.. |PyPIStatus| image:: https://badge.fury.io/py/queue-map-reduce-relleums.svg
   :target: https://pypi.org/project/queue-map-reduce-relleums

.. |BlackStyle| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black

.. Dask_