import os, argparse, itertools, pickle
import numpy as np
import ARAUtils, preprocessing

def correlate_and_average(df, channel_a, channel_b, preprocessor = lambda sig: sig, time_branch = "t_ns", entry_branch = "entry"):

    grouped = df.groupby([entry_branch])
    available_entries = list(grouped.indices.keys())

    correlators = []
    
    for cur_entry in available_entries:
        event = grouped.get_group(cur_entry)
        cur_t_vals = event[time_branch].to_numpy(dtype = float)
        cur_sig_vals_a = preprocessor(event[f"ch_{channel_a}"].to_numpy(dtype = float))
        cur_sig_vals_b = preprocessor(event[f"ch_{channel_b}"].to_numpy(dtype = float))
        
        cur_correlator = np.correlate(cur_sig_vals_a, cur_sig_vals_b, mode = "full")
        correlators.append(cur_correlator)

    number_correlators = len(correlators)
    correlator_mean = np.mean(correlators, axis = 0)
    correlator_var = np.var(correlators, axis = 0)

    return number_correlators, correlator_mean, correlator_var

def correlate_runs(indir, outdir, runs_to_process, channels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                   trigger = "forced"):

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    def preprocessor(sig):
        passbands = [(150e6, 350e6), (500e6, 700e6)]        
        return preprocessing.band_limit(preprocessing.ensure_zero_mean(sig),
                                        passbands = passbands,
                                        fs = 1.5e9)
        
    for cur_run in runs_to_process:
        
        selectors = {
            "forced": lambda header: header.trigger_type == 1
            "calib": lambda header: header.trigger_type == 2 and header.gate_flag
        }
        
        run_df = ARAUtils.extract_events_from_run(indir, run = cur_run, channels = channels, selector = selectors[trigger])

        correlators = {}
        for (channel_a, channel_b) in itertools.combinations(channels, 2):
            pairing = f"channel_{channel_a}_{channel_b}"
            correlators[pairing] = {}
            outdict = correlators[pairing]
            
            number_correlators, correlator_mean, correlator_var = correlate_and_average(run_df, channel_a, channel_b,
                                                                                        preprocessor = preprocessor)
            outdict["num_corr"] = number_correlators
            outdict["mean"] = correlator_mean
            outdict["var"] = correlator_var
        
        outpath = os.path.join(outdir, f"run_{cur_run}.pkl")
        with open(outpath, 'wb') as outfile:
            pickle.dump(correlators, outfile)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    parser.add_argument("--trigger", action = "store", dest = "trigger")
    parser.add_argument("--runs", action = "store", nargs = "+", dest = "runs_to_process", type = int)
    args = vars(parser.parse_args())

    correlate_runs(**args)
