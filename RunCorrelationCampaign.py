import os, argparse, uuid, textwrap
import JobUtils

def write_job_script(submitdir, outdir, runs, trigger):

    if not os.path.exists(submitdir):
        os.makedirs(submitdir)
    
    sceleton = """\
    #!/bin/bash
    
    source {rootdir}/setup.sh
    python3 {rootdir}/correlate_runs.py --outdir {outdir} --runs {runs} --trigger {trigger}
    """

    job_name = str(uuid.uuid4())
    scriptpath = os.path.join(submitdir, f"{job_name}.sh")
    with open(scriptpath, 'w') as scriptfile:
        scriptfile.write(textwrap.dedent(sceleton.format(
            rootdir = os.environ["ROOTDIR"], outdir = outdir, trigger = trigger,
            runs = " ".join(map(str, runs))
        )))

    return job_name, scriptpath    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    parser.add_argument("--runlist", action = "store", dest = "runlist_path")
    parser.add_argument("--trigger", action = "store", default = "forced", dest = "trigger")
    parser.add_argument("--runs_per_job", action = "store", dest = "runs_per_job", type = int, default = 10)
    parser.add_argument("--dryrun", action = "store_true", default = False, dest = "dryrun")
    args = vars(parser.parse_args())

    JobUtils.run_campaign(write_job_script, **args)
