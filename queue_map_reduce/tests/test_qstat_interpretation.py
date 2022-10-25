import queue_map_reduce as qmr
import pickle
import numpy
import os
import subprocess


def test_filter_JB_name():
    JB_names_of_my_jobs = ["11", "12", "13"]
    JB_names_of_all_jobs = [
        "fish",
        "13",
        "mouse",
        "7",
        "not12",
        "12not",
        "12",
        "dog",
        "11",
        "cat",
    ]
    all_jobs = [{"JB_name": name} for name in JB_names_of_all_jobs]

    my_JB_names_set = set(JB_names_of_my_jobs)

    my_jobs = qmr.queue.job_organization.filter_jobs_by_JB_name(
        jobs=all_jobs, JB_names_set=my_JB_names_set,
    )

    assert len(my_jobs) == 3
    for job in my_jobs:
        assert job["JB_name"] in my_JB_names_set


def test_extract_error_state_no_errors():
    jobs_running = [{"state": "r"} for i in range(42)]
    jobs_pending = [{"state": "qw"} for i in range(1337)]
    r, p, e = qmr.queue.job_organization.extract_error_from_running_pending(
        jobs_running=jobs_running,
        jobs_pending=jobs_pending,
        error_state_indicator="E",
    )
    assert len(r) == 42
    assert len(p) == 1337
    assert len(e) == 0
    for jr in r:
        assert jr["state"] == "r"
    for jp in p:
        assert jp["state"] == "qw"


def test_extract_error_state_with_errors():
    jobs_running = []
    for i in range(1000):
        job = {
            "state": "Er" if i % 10 == 0 else "r",
            "JB_job_number": str(i),
        }
        jobs_running.append(job)
    jobs_pending = []
    for i in range(2000):
        job = {
            "state": "Eqw" if i % 10 == 0 else "qw",
            "JB_job_number": str(i),
        }
        jobs_pending.append(job)

    r, p, e = qmr.queue.job_organization.extract_error_from_running_pending(
        jobs_running=jobs_running,
        jobs_pending=jobs_pending,
        error_state_indicator="E",
    )
    assert len(r) == 900
    assert len(p) == 1800
    assert len(e) == 300


def test_job_name_ichunk():
    ichunks = [
        1000 * 1000 * 1000 * i + 1000 * 1000 * i + 1000 * i + i
        for i in range(1000)
    ]
    for ichunk in ichunks:
        JB_name = qmr.utils.make_JB_name_from_ichunk(
            session_id="hans", ichunk=ichunk,
        )
        ichunk_back = qmr.utils.make_ichunk_from_JB_name(JB_name=JB_name)
        assert ichunk_back == ichunk
