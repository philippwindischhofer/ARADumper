import os, re, argparse, pickle, datetime, combine_correlators
import pandas as pd
import numpy as np

def extract_run(path):
    run_extractor = re.compile("run_(.+).pkl")
    m = re.search(run_extractor, os.path.basename(path))
    if m:
        return int(m.group(1))
    else:
        raise RuntimeError("Error: unexpected naming convention!")

def get_run_timestamp(df, run):
    df_run = df.loc[df["run"] == run]
    assert len(df_run) == 1
    return df_run[["readout_time"]].to_numpy().flatten()[0]

def get_run_time(df, run):
    return datetime.datetime.fromisoformat(get_run_timestamp(df, run))

def partition_runs(runs, run_times, num_days):
    runs = np.array(runs)
    runlength_days = np.array(np.diff(run_times) / datetime.timedelta(days = 1))
    runs_per_partition = (np.cumsum(runlength_days) / num_days).astype(dtype = int)
    runs_per_partition = np.append(runs_per_partition, runs_per_partition[-1])
    
    partitioned_runs = []
    partitioned_start_date = []
    for part_ind in set(runs_per_partition):
        runs_in_partition = runs[runs_per_partition == part_ind]
        partitioned_runs.append(runs_in_partition)
    
    return partitioned_runs

def get_runfile(inpaths, run):
    for path in inpaths:
        if f"run_{run}.pkl" in path:
            return path
    raise RuntimeError("Internal error")

def combine_correlators_by_rule(outdir, inpaths, metapath, num_days, verbose = True, dryrun = True):
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    df_meta = pd.read_csv(metapath)
    runs_on_disk = set([extract_run(path) for path in inpaths])
    runs_from_metadata = set(df_meta["run"].to_numpy().flatten())

    runs_available = list(sorted(list(runs_on_disk.intersection(runs_from_metadata))))
    run_times = [get_run_time(df_meta, run) for run in runs_available]

    partitions = partition_runs(runs_available, run_times, num_days)
    partition_times = [get_run_timestamp(df_meta, partition[0]) for partition in partitions]
    partition_index = list(range(len(partition_times)))

    meta_outpath = os.path.join(outdir, "partitions.csv")
    partition_meta = pd.DataFrame.from_dict({"partition_index": partition_index,
                                             "partition_time": partition_times})
    if not dryrun:
        partition_meta.to_csv(meta_outpath, index = False)
    
    for partition_index, partition_time, runs in zip(partition_index, partition_times, partitions):
        partition_outpath = os.path.join(outdir, f"partition_{partition_index}.pkl")
        partition_inpaths = [get_runfile(inpaths, run) for run in runs]

        if verbose:
            print("-" * 50)
            print(f"Writing partition {partition_index} to {partition_outpath}")
            print("\n".join(partition_inpaths))
            print("-" * 50)

        if not dryrun:
            combine_correlators.combine_correlators(partition_outpath, partition_inpaths)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--infiles", action = "store", dest = "inpaths", nargs = '+')
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    parser.add_argument("--metadata", action = "store", dest = "metapath")
    parser.add_argument("--num_days", action = "store", type = int, dest = "num_days")
    parser.add_argument("--dryrun", action = "store_true", default = False, dest = "dryrun")
    args = vars(parser.parse_args())

    combine_correlators_by_rule(**args)
