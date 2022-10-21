from queue_map_reduce import tools as qmr_tools
import pytest


def _flatten(bundles):
    flat = []
    for b in bundles:
        flat += b
    return flat


def test_zero_jobs_and_valid_num_bundles():
    for num_bundles in [None, 1, 100]:
        bundles = qmr_tools.assign_jobs_to_bundles(num_jobs=0, num_bundles=num_bundles)
        assert len(bundles) == 0


def test_zero_jobs_and_bad_num_bundles():
    with pytest.raises(AssertionError):
        bundles = qmr_tools.assign_jobs_to_bundles(num_jobs=0, num_bundles=0)

    with pytest.raises(AssertionError):
        bundles = qmr_tools.assign_jobs_to_bundles(num_jobs=0, num_bundles=-1)


def test_many_jobs_one_bundle():
    bundles = qmr_tools.assign_jobs_to_bundles(num_jobs=100, num_bundles=1)
    assert len(bundles) == 1

    jobs_back = _flatten(bundles)

    for j in range(100):
        assert jobs_back[j] == j


def test_many_jobs_many_bundles():
    bundles = qmr_tools.assign_jobs_to_bundles(num_jobs=1000, num_bundles=10)
    assert len(bundles) == 10

    jobs_back = _flatten(bundles)

    for j in range(1000):
        assert jobs_back[j] == j


def test_few_jobs_many_bundles():
    bundles = qmr_tools.assign_jobs_to_bundles(num_jobs=10, num_bundles=100)
    assert len(bundles) == 10

    jobs_back = _flatten(bundles)

    for j in range(10):
        assert jobs_back[j] == j
