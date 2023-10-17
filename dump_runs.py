import sys, os, glob, argparse
import pandas as pd
from ARAReader import Reader

def get_available_runs(dirpath):
    all_run_dirs = list(filter(lambda folder: "run" in folder, os.listdir(dirpath)))
    run_numbers = list(map(lambda folder: int(folder.replace("run", "")), all_run_dirs))
    return run_numbers

def extract_events_from_run(base_dir, run, channels = [3], selector = lambda header: True):
    reader = Reader(run, base_dir)

    event_dfs = []
    
    number_entries = reader.numberEntries()
    print(f"Have {number_entries} entries in this run")
    for cur_entry in range(number_entries):
        reader.setEntry(cur_entry)
    
        header = reader.header()

        if not selector(header):
            continue

        event_data = {}
        event_data["t_ns"] = reader.t()

        for channel in channels:
            event_data[f"ch_{channel}"] = reader.wf(ch = channel)

        event_df = pd.DataFrame(event_data)
        event_df["entry"] = cur_entry
        print(event_df)

        break
    
def dump_runs(indir, outdir):

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # available_runs = get_available_runs(indir)        

    extract_events_from_run(indir, run = 12105, channels = [0, 1, 2, 3, 4, 5, 6, 7])

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    args = vars(parser.parse_args())

    dump_runs(**args)
