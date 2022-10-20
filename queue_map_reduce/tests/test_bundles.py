import queue_map_reduce as qmr
from queue_map_reduce import tools as qmr_tools
import numpy as np
import pytest


def _flatten(bundles):
    flat = []
    for b in bundles:
        flat += b
    return flat


def test_zero_jobs_and_valid_num_bundles():
    for num_bundles in [None, 1, 100]:
        bundles = qmr_tools.bundle_jobs(jobs=[], num_bundles=num_bundles)
        assert len(bundles) == 0


def test_zero_jobs_and_bad_num_bundles():
    with pytest.raises(AssertionError):
        bundles = qmr_tools.bundle_jobs(jobs=[], num_bundles=0)

    with pytest.raises(AssertionError):
        bundles = qmr_tools.bundle_jobs(jobs=[], num_bundles=-1)


def test_many_jobs_one_bundle():
    jobs = np.arange(1000).tolist()
    bundles = qmr_tools.bundle_jobs(jobs=jobs, num_bundles=1)
    assert len(bundles) == 1

    jobs_back = _flatten(bundles)

    for j in range(len(jobs)):
        assert jobs_back[j] == jobs[j]


def test_many_jobs_many_bundles():
    jobs = np.arange(1000).tolist()
    bundles = qmr_tools.bundle_jobs(jobs=jobs, num_bundles=10)
    assert len(bundles) == 10

    jobs_back = _flatten(bundles)

    for j in range(len(jobs)):
        assert jobs_back[j] == jobs[j]


def test_few_jobs_many_bundles():
    jobs = np.arange(10).tolist()
    bundles = qmr_tools.bundle_jobs(jobs=jobs, num_bundles=100)
    assert len(bundles) == 10

    jobs_back = _flatten(bundles)

    for j in range(len(jobs)):
        assert jobs_back[j] == jobs[j]


def _run_job_example(job):
    """
    For testing. This function processes one job.
    You already have this for your jobs.
    """
    return job * job


def _run_jobs_in_bundles_example(bundle):
    """
    For testing. This function processes one bundle of jobs by
    looping over the jobs in a bundle.
    You need to provide this function.
    """
    results = []
    for j, job in enumerate(bundle):
        result = _run_job_example(job=job)
        results.append(result)
    return results


def test_run_jobs_in_bundles():
    jobs = np.arange(24).tolist()
    bundles = qmr_tools.bundle_jobs(jobs=jobs, num_bundles=3)

    bundles_results = []
    for bundle in bundles:
        bundle_results = _run_jobs_in_bundles_example(bundle=bundle)
        bundles_results.append(bundle_results)

    job_results = _flatten(bundles_results)

    for j in range(len(jobs)):
        assert job_results[j] == _run_job_example(jobs[j])
