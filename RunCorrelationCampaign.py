import os, argparse, uuid, textwrap
import pandas as pd
from SlurmJobSubmitter import SlurmJobSubmitter

def write_job_script(submitdir, outdir, runs):

    if not os.path.exists(submitdir):
        os.makedirs(submitdir)
    
    sceleton = """\
    #!/bin/bash
    
    source {rootdir}/setup.sh
    python3 {rootdir}/correlate_runs.py --outdir {outdir} --runs {runs}
    """

    job_name = str(uuid.uuid4())
    scriptpath = os.path.join(submitdir, f"{job_name}.sh")
    with open(scriptpath, 'w') as scriptfile:
        scriptfile.write(textwrap.dedent(sceleton.format(
            rootdir = os.environ["ROOTDIR"], outdir = outdir,
            runs = " ".join(map(str, runs))
        )))

    return job_name, scriptpath

def run_correlation_campaign(outdir, indir, runlist_path, runs_per_job, dryrun):

    runs_to_process = list(pd.read_csv(runlist_path)["run"])
    print(f"Have {len(runs_to_process)} runs to process")
    
    runs_for_jobs = [runs_to_process[start:start+runs_per_job] for start in range(0, len(runs_to_process), runs_per_job)]
    
    submitdir = os.path.join(outdir, "submit")
    
    for cur_runs in runs_for_jobs:
        job_name, scriptpath = write_job_script(submitdir, outdir, cur_runs)
        SlurmJobSubmitter.submit(cmds = [f"sh {scriptpath}"], submitdir = submitdir, dryrun = dryrun,
                                 job_name = job_name)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    parser.add_argument("--runlist", action = "store", dest = "runlist_path")
    parser.add_argument("--runs_per_job", action = "store", dest = "runs_per_job", type = int, default = 10)
    parser.add_argument("--dryrun", action = "store_true", default = False, dest = "dryrun")
    args = vars(parser.parse_args())

    run_correlation_campaign(**args)

    
