import os, argparse, pickle
import pandas as pd
import numpy as np

def read_correlators(path):
    with open(path, 'rb') as infile:
        cur_data = pickle.load(infile)
        return cur_data

def stack_correlators(indir, outpath, stackpath):
    stack_meta = pd.read_csv(stackpath)
    partition_index = stack_meta[["partition_index"]].to_numpy().flatten()
    partition_time = stack_meta[["partition_time"]].to_numpy().flatten()
    
    correlators = {}
    for index, time in zip(partition_index, partition_time):
        cur_inpath = os.path.join(indir, f"partition_{index}.pkl")
        partition_correlators = read_correlators(cur_inpath)
        for pairing_name, pairing_correlator in partition_correlators.items():
            if not pairing_name in correlators:
                correlators[pairing_name] = []
            correlators[pairing_name].append(pairing_correlator["mean"])
    
    stacked_correlators = {}
    for pairing_name, pairing_correlator in correlators.items():
        stacked_correlators[pairing_name] = np.stack(pairing_correlator)    
        
    with open(outpath, 'wb') as outfile:
        pickle.dump({"partition_time": partition_time, "stacked_correlator": stacked_correlators},
                    outfile)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir")
    parser.add_argument("--outfile", action = "store", dest = "outpath")
    parser.add_argument("--stackfile", action = "store", dest = "stackpath")
    args = vars(parser.parse_args())

    stack_correlators(**args)
