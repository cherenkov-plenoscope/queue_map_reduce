#!/usr/bin/env python3
import os
import sys
import json
import pkg_resources
import datetime
import subprocess
import sun_grid_engine_map as qmr


def job_to_xml(job):
    jld = ''
    jld += '<job_list state="{:s}">\n'.format(job["@state"])
    jld += '    <JB_job_number>{:s}</JB_job_number>\n'.format(
        job["JB_job_number"])
    jld += '    <JAT_prio>{:s}</JAT_prio>\n'.format(job["JAT_prio"])
    jld += '    <JB_name>{:s}</JB_name>\n'.format(job["JB_name"])
    jld += '    <JB_owner>{:s}</JB_owner>\n'.format(job["JB_owner"])
    jld += '    <state>{:s}</state>\n'.format(job["state"])
    jld += '    <JB_submission_time>{:s}</JB_submission_time>\n'.format(
        job["JB_submission_time"])
    jld += '    <queue_name>{:s}</queue_name>\n'.format(job["queue_name"])
    jld += '    <slots>{:s}</slots>\n'.format(job["slots"])
    jld += '</job_list>\n'
    return jld


def state_to_xml(state):
    out_xml = "<?xml version='1.0'?>\n"
    out_xml += '<job_info>\n'

    out_xml += '    <queue_info>\n'
    for job in state['running']:
        out_xml += indent_text(job_to_xml(job), indent=8)
    out_xml += '    </queue_info>\n'

    out_xml += '    <job_info>\n'
    for job in state['pending']:
        out_xml += indent_text(job_to_xml(job), indent=8)
    out_xml += '    </job_info>\n'

    out_xml += '</job_info>\n'
    return out_xml


def indent_text(text, indent=4):
    out = []
    spaces = " "*indent
    for line in text.splitlines():
        out.append(spaces + line + "\n")
    return ''.join(out)



def actually_run_the_job(job):
    with open(job["_opath"], "wt") as o, open(job["_epath"], "wt") as e:
        subprocess.call(
            [job["_python_path"], job["_script_arg_0"], job["_script_arg_1"]],
            stdout=o,
            stderr=e,
        )

# dummy qstat
# ===========
# Every time dummy qsub is called, it runs one job.

assert(len(sys.argv) == 2)
assert sys.argv[1] == "-xml"

tmp_path = pkg_resources.resource_filename(
    'sun_grid_engine_map',
    os.path.join('test', 'resources', '_tmp_qsub_state.json')
)

with open(tmp_path, "rt") as f:
    state = json.loads(f.read())

MAX_NUM_RUNNING = 10
EVIL_JOB_IDX = 13
MAX_NUM_FAILS_OF_EVIL_JOB = 5

if len(state["running"]) >= MAX_NUM_RUNNING:
    run_job = state["running"].pop(0)
    actually_run_the_job(run_job)
elif len(state["pending"]) > 0:
    job = state["pending"].pop(0)
    if qmr._idx_from_JB_name(job["JB_name"]) == EVIL_JOB_IDX:
        if state["num_fails_of_evil_job"] <= MAX_NUM_FAILS_OF_EVIL_JOB:
            job["@state"] = "?"
            job["state"] = "Eqw"
            state["pending"].append(job)
            state["num_fails_of_evil_job"] += 1
        else:
            job["@state"] = "running"
            job["state"] = "r"
            state["running"].append(job)
    else:
        job["@state"] = "running"
        job["state"] = "r"
        state["running"].append(job)
elif len(state["running"]) > 0:
    run_job = state["running"].pop(0)
    actually_run_the_job(run_job)

with open(tmp_path, "wt") as f:
    f.write(json.dumps(state, indent=4))

out_xml = state_to_xml(state)
print(out_xml)

sys.exit(0)
