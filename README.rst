================
queue map reduce
================

|TravisBuildStatus| |PyPIStatus| |BlackStyle|

Job-queues, or batch-jobs are a powerful tool to distribute your compute-jobs over multiple machines in parallel. This single ``python``-function maps your compute-jobs onto a queue, and reduces the results.

.. code:: python

    import queue_map_reduce as qmr
    import numpy

    results = qmr.map(
        function=numpy.sum,
        jobs=[numpy.arange(i, 100+i) for i in range(10)]
    )

The interface is inspired by the stdandard ``map()`` and ``multiprocessing.Pool``'s ``map()``.

Requirements
============

- Programs ``qsub``, ``qstat``, and ``qdel`` are required to submit, monitor, and delete jobs.

- Your ``function(job)`` must be part of an importable python module.

- Your ``jobs`` and their ``results`` must be serializable using pickle.

- All worker-nodes and the process-node itself running the ``map()`` must share a common path in the file-system were they can read and write.

Queue flavor
------------
Different flavors of ``qsub``, ``qstat``, and ``qdel`` might have different interfaces. Tested flavors are:

- Sun Grid Engine (SGE) 8.1.9

Features
--------
- Only one funtion to map and reduce in a queue.

- Jobs that run into states ``'E'`` will be deleted and resubmitted until an upper limit is reached.


Inner workings
==============
- Our ``map()`` creates a working directory as the mapping and reduction takes place in the file-system. You can set this ``work_dir`` manually to ensure it is in a common path of all worker-nodes and the process-node.

- Our ``map()`` serializes each of your ``jobs`` using ``pickle`` into a seperate file in the ``work_dir`` in ``work_dir/{:09d}.pkl``.

- Our ``map()`` reads all the environment-variables in its process.

- Our ``map()`` creates the worker-node-script in ``work_dir/worker_node_script.py``. This ``python`` script contains and exports the process' environment-variables into the batch-job's constext. It reads the job from the ``work_dir`` in ``work_dir\{:09d}.pkl``, imports and runs your ``result = function(job)``, and finally writes the result back into ``work_dir\{:09d}.pkl.out``.

- Our ``map()`` starts to sbmit your jobs into the queue using ``qsub``. The ``stdout`` and ``stderr`` of the jobs are written to ``work_dir/{:09d}.pkl.o`` and ``work_dir/{:09d}.pkl.e`` respectively. By default, ``qsub`` is told to use ``shutil.which("python")`` to process the worker-node-script.

- When all jobs are submitted, ``map()`` monitors its jobs using ``qstat``. In case a job will run into an error-state, which is ``'E'`` by default, the job will be deleted and resubmitted until a maximum number of resubmissions is reached.

- When no more jobs are running or pending, ``map()`` will start to reduce the results. It will read each result from ``work_dir/{:09d}.pkl.out`` and append it to the list of results.

- In case of non zero ``stderr`` in any job, a missing result, or on the users request, the ``work_dir`` will be kept for inspection. Otherwise its removed.


Identifying jobs
----------------
- ``JB_job_number`` is assigned to your job by the queue-system for its own book-keeping.

- ``JB_name`` is the name assigned to your job by our ``map()``. It is composed of a session-name with an iso-time-stamp, and the ``idx`` of your job with respect to your lists of jobs. E.g. ``"q"%Y-%m-%dT%H:%M:%S"#{:09d}"``

- ``idx`` is only used within our ``map()``. It is the index of your job with respect to your list of jobs. It is written into ``JB_name``, and it is the ``idx`` used to create the job's filenames in the ``work_dir`` such as ``work_dir/{:09d}.pkl``.


Environment Variables
---------------------
All the users environment-variables in the process where ``queue_map_and_reduce.map()`` is called will be exported in the job's context.

The ``python``-script executed on the worker-nodes will set the environment-variables before calling ``function(job)``. Here we do not rely on ``qsub``'s argument ``-V`` because on some clusters this will _not_ set _all_ variables. Apparently some admins fear security issues when using ``qsub -V`` to set ``LD_LIBRARY_PATH``.

Testing
=======

.. code:: bash

    py.test -s .


dummy queue
-----------
To test our ``map()`` we provide a dummy ``qsub``, ``qstat``, and ``qdel``.
These are individual ``python``-scripts which all act on a common state-file in ``tests/resources/dummy_queue_state.json``.

Here ``dummy_qsub.py`` only appends jobs to the list of pending jobs in the state-file.

The ``qdummy_del.py`` only removes jobs from the state-file.

And ``dummy_qstat.py`` does move the jobs from the pending to the running list, and does trigger the actual processing of the jobs. Each time ``dummy_qstat.py`` is called it performs a single action on the state-file. So it must be called multiple times to process all jobs.

Before running the dummy-queue, this state-file must be initialized using:

.. code:: python

    from queue_map_reduce import dummy_queue

    dummy_queue.init_queue_state(
        path="tests/resources/dummy_queue_state.json"
    )

Now when testing you point our ``map()`` to our dummy-queue. The dummy-queue can also intentionally bring jobs into the error-state.

See ``tests/test_full_chain_with_dummy_qsub.py``.

Because of the global state-fiel, only one dummy_queue must run at a time.


.. |TravisBuildStatus| image:: https://travis-ci.org/cherenkov-plenoscope/queue_map_reduce.svg?branch=master
   :target: https://travis-ci.org/cherenkov-plenoscope/queue_map_reduce

.. |PyPIStatus| image:: https://badge.fury.io/py/NOT_YET_ON_PYPI.svg
   :target: https://pypi.python.org/pypi/NOT_YET_ON_PYPI

.. |BlackStyle| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
