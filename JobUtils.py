import pandas as pd
import os
from SlurmJobSubmitter import SlurmJobSubmitter

def run_campaign(job_script_writer, outdir, indir, runlist_path, runs_per_job, dryrun):

    runs_to_process = list(pd.read_csv(runlist_path)["run"])
    print(f"Have {len(runs_to_process)} runs to process")
    
    runs_for_jobs = [runs_to_process[start:start+runs_per_job] for start in range(0, len(runs_to_process), runs_per_job)]
    
    submitdir = os.path.join(outdir, "submit")
    
    for cur_runs in runs_for_jobs:
        job_name, scriptpath = job_script_writer(submitdir, outdir, cur_runs)
        SlurmJobSubmitter.submit(cmds = [f"sh {scriptpath}"], submitdir = submitdir, dryrun = dryrun,
                                 job_name = job_name)
