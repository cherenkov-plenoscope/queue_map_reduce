====================
queue map and reduce
====================

|TravisBuildStatus| |PyPIStatus| |BlackStyle|

Job-queues, or batch-jobs are a powerful tool to distribute your compute-jobs over multiple machines in parallel. This single ``python``-function maps your compute-jobs onto a queue, and reduces the results.

.. code:: python

    import sun_grid_engine_map as qmr
    import numpy

    results = qmr.map(
        function=numpy.sum,
        jobs=[numpy.arange(i, 100+i) for i in range(10)]
    )

The interface is inspired by  in ``multiprocessing.Pool``'s ``map()``.

Requirements
------------

- Programs ``qsub``, ``qstat``, and ``qdel`` are required to submit, monitor, and delete jobs.

- Your ``function(job)`` must be part of an importable python module.

- Your ``jobs`` and their ``results`` must be serializable using pickle.

- All worker-nodes and the process-node itself running the ``map()`` must share a common path in the file-system were they can read and write.

Queue flavor
------------
Different flavors of ``qsub``, ``qstat``, and ``qdel`` might have different interfaces. Tested flavors are:

- Sun Grid Engine (SGE) 8.1.9

Inner workings
--------------
- Our ``map()`` creates a working directory as the mapping and reduction takes place in the file-system. You can set this ``work_dir`` manually to ensure it is in a common path of all worker-nodes and the process-node.

- Our ``map()`` serializes each of your ``jobs`` using ``pickle`` into a seperate file in the ``work_dir`` in ``work_dir/{:09d}.pkl``.

- Our ``map()`` reads all the environment-variables in its process.

- Our ``map()`` creates the worker-node-script in ``work_dir/worker_node_script.py``. This ``python`` script contains and exports the process' environment-variables into the batch-job's constext. It reads the job from the ``work_dir`` in ``work_dir\{:09d}.pkl``, imports and runs your ``result = function(job)``, and finally writes the result back into ``work_dir\{:09d}.pkl.out``.

- Our ``map()`` starts to sbmit your jobs into the queue using ``qsub``. The ``stdout`` and ``stderr`` of the jobs are written to ``work_dir/{:09d}.pkl.o`` and ``work_dir/{:09d}.pkl.e`` respectively. By default, ``qsub`` is told to use ``shutil.which("python")`` to process the worker-node-script.

- When all jobs are submitted, ``map()`` monitors its jobs using ``qstat``. In case a job will run into an error-state, which is ``'E'`` by default, the job will be deleted and resubmitted until a maximum number of resubmissions is reached.

- When no more jobs are running or pending, ``map()`` will start to reduce the results. It will read each result from ``work_dir/{:09d}.pkl.out`` and append it the the list of results.

- In case of non zero ``stderr`` in any job, a missing result, or on the users request, the ``work_dir`` will be kept for inspection. Otherwise its removed.


``qsub``
    The command ``qsub -o -e -N -S`` is used to submit your job into the queue. It must support arguments ``-o`` and ``-e`` to write ``std-out`` and ``std-err`` to files. Argument ``-N`` is the ``JB_name`` created by ``queue_map_and_reduce``, and ``-S`` is the path of the program that will be invoked to process the script of the batch-job.

``qstat``
    The ``qstat -xml`` is used to monitor the state of your jobs while they are pending, running, or in an error-state. To parse the ``xml`` we use the ``qstat`` module.

    .. code:: xml

        <job_list state="pending">
            <JB_job_number>1337</JB_job_number>
            <JAT_prio>0.00000</JAT_prio>
            <JB_name>q2019-06-23T13:24:83#00001</JB_name>
            <JB_owner>corona</JB_owner>
            <state>qw</state>
            <JB_submission_time>2019-06-23T13:28:09</JB_submission_time>
            <queue_name></queue_name>
            <slots>1</slots>
        </job_list>




``qdel JB_job_number`` is used to delete your jobs in case they run into error-states.


Environment Variables
---------------------
All the users environment-variables in the process where ``queue_map_and_reduce.map()`` is called will be exported in the job's context.

The ``python``-script executed on the worker-nodes will set the environment-variables before calling ``function(job)``. Here we do not rely on ``qsub's`` argument ``-V`` because on some clusters this will _not_ set _all_ variables. Apparently some admins fear security issues when using ``qsub -V`` to set ``LD_LIBRARY_PATH``.


dummy queue for testing
-----------------------
To test our ``map()`` we provide a dummy ``qsub``, ``qstat``, and ``qdel``.


.. |TravisBuildStatus| image:: https://travis-ci.org/cherenkov-plenoscope/sun_grid_engine_map.svg?branch=master
   :target: https://travis-ci.org/cherenkov-plenoscope/sun_grid_engine_map

.. |PyPIStatus| image:: https://badge.fury.io/py/NOT_YET_ON_PYPI.svg
   :target: https://pypi.python.org/pypi/NOT_YET_ON_PYPI

.. |BlackStyle| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
