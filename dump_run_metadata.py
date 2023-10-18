import os, argparse, datetime
from ARAReader import Reader
import pandas as pd

def get_available_runs(dirpath):
    all_run_dirs = list(filter(lambda folder: "run" in folder, os.listdir(dirpath)))
    run_numbers = list(map(lambda folder: int(folder.replace("run", "")), all_run_dirs))
    return run_numbers

def dump_metadata(indir, outpath):
    available_runs = get_available_runs(indir)
    number_runs = len(available_runs)

    metadata = {"run": [], "readout_time": []}
    
    for cur_ind, cur_run in enumerate(available_runs):

        print(f"Peeking inside run {cur_run} ({cur_ind} / {number_runs})")

        # use the first event to assign a time to the entire run
        reader = Reader(cur_run, indir)
        reader.setEntry(0)
        
        header = reader.header()
        readout_time = datetime.datetime.fromtimestamp(header.getReadoutTimeFloat(), datetime.timezone.utc)

        metadata["run"].append(cur_run)
        metadata["readout_time"].append(readout_time.isoformat())
        
    meta_df = pd.DataFrame(metadata)
    meta_df = meta_df.sort_values(by = "run", ascending = True)
    meta_df.to_csv(outpath, index = False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outpath", action = "store", dest = "outpath")
    args = vars(parser.parse_args())

    dump_metadata(**args)

