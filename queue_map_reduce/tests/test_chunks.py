from queue_map_reduce import tools as qmr_tools
import pytest


def _flatten(chunks):
    flat = []
    for b in chunks:
        flat += b
    return flat


def test_zero_jobs_and_valid_num_chunks():
    for num_chunks in [None, 1, 100]:
        chunks = qmr_tools.assign_jobs_to_chunks(
            num_jobs=0, num_chunks=num_chunks
        )
        assert len(chunks) == 0


def test_zero_jobs_and_bad_num_chunks():
    with pytest.raises(AssertionError):
        chunks = qmr_tools.assign_jobs_to_chunks(num_jobs=0, num_chunks=0)

    with pytest.raises(AssertionError):
        chunks = qmr_tools.assign_jobs_to_chunks(num_jobs=0, num_chunks=-1)


def test_many_jobs_one_bundle():
    chunks = qmr_tools.assign_jobs_to_chunks(num_jobs=100, num_chunks=1)
    assert len(chunks) == 1

    ijobs = _flatten(chunks)

    for j in range(100):
        assert ijobs[j] == j


def test_many_jobs_many_chunks():
    chunks = qmr_tools.assign_jobs_to_chunks(num_jobs=1000, num_chunks=10)
    assert len(chunks) == 10

    ijobs = _flatten(chunks)

    for j in range(1000):
        assert ijobs[j] == j


def test_few_jobs_many_chunks():
    chunks = qmr_tools.assign_jobs_to_chunks(num_jobs=10, num_chunks=100)
    assert len(chunks) == 10

    ijobs = _flatten(chunks)

    for j in range(10):
        assert ijobs[j] == j
