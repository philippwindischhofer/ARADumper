import argparse, os, pickle
import numpy as np
import ARAUtils

def make_psd(df, channel, fs = 1.5e9, preprocessor = lambda sig: sig, time_branch = "t_ns", entry_branch = "entry", one_sided = True):

    grouped = df.groupby([entry_branch])
    available_entries = list(grouped.indices.keys())

    fft_freqs = None
    fft_mag_sq = []
    
    for cur_entry in available_entries:
        event = grouped.get_group(cur_entry)
        cur_t_vals = event[time_branch].to_numpy(dtype = float)
        cur_sig_vals = preprocessor(event[f"ch_{channel}"].to_numpy(dtype = float))

        if fft_freqs is None:
            fft_freqs = np.fft.fftfreq(n = len(cur_sig_vals), d = 1 / fs)
        
        cur_fft = np.fft.fft(cur_sig_vals)
        fft_mag_sq.append(np.square(np.abs(cur_fft)))

    number_waveforms = len(fft_mag_sq)
    psd = np.mean(fft_mag_sq, axis = 0)
    if one_sided:
        one_sided_mask = fft_freqs > 0
        psd_freqs = fft_freqs[one_sided_mask]
        psd = 2 * psd[one_sided_mask]
    else:
        psd_freqs = fft_freqs

    return number_waveforms, psd_freqs, psd

def psd_runs(indir, outdir, runs_to_process, channels = [0, 1, 2, 3, 4, 5, 6, 7]):

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    for cur_run in runs_to_process:
        
        keep_forced_trigger = lambda header: header.trigger_type == 1
        run_df = ARAUtils.extract_events_from_run(indir, run = cur_run, channels = channels, selector = keep_forced_trigger)

        psds = {}
        for channel in channels:
            label = f"channel_{channel}"
            psds[label] = {}
            outdict = psds[label]

            number_waveforms, psd_freqs, psd = make_psd(run_df, channel)
            outdict["num_waveforms"] = number_waveforms
            outdict["freqs"] = psd_freqs
            outdict["psd"] = psd

        outpath = os.path.join(outdir, f"run_{cur_run}.pkl")
        with open(outpath, 'wb') as outfile:
            pickle.dump(psds, outfile)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--indir", action = "store", dest = "indir", default = "/project2/kicp/cozzyd/nuphase-root-data/")
    parser.add_argument("--outdir", action = "store", dest = "outdir")
    parser.add_argument("--runs", action = "store", nargs = "+", dest = "runs_to_process", type = int)
    args = vars(parser.parse_args())

    psd_runs(**args)
