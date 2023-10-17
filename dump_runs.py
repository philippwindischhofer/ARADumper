import sys, os, glob, argparse
from ARAReader import Reader

def get_available_runs(dirpath):
    all_run_dirs = list(filter(lambda folder: "run" in folder, os.listdir(dirpath)))
    run_numbers = list(map(lambda folder: int(folder.replace("run", "")), all_run_dirs))
    return run_numbers

def dump_runs(indir, outdir):

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    available_runs = get_available_runs(indir)

    print(available_runs)
        
    evt = Reader(run = 999, base_dir = indir)
    # evt.header().Dump()
        
    # print("done")    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    args = vars(parser.parse_args())

    dump_runs(**args)
