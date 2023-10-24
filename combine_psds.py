import os, argparse, pickle
import numpy as np

def combine_psds(outpath, inpaths):

    combined_psds = {}

    for cur_inpath in inpaths:
        with open(cur_inpath, 'rb') as cur_infile:
            cur_data = pickle.load(cur_infile)

            for cur_label, cur_psd in cur_data.items():
                if cur_label not in combined_psds:
                    combined_psds[cur_label] = {}                
                
                indict = cur_data[cur_label]
                outdict = combined_psds[cur_label]

                if "num_waveforms" not in outdict:
                    outdict["num_waveforms"] = indict["num_waveforms"]
                    outdict["freqs"] = indict["freqs"]
                    outdict["psd"] = indict["psd"]
                else:
                    num_accum = outdict["num_waveforms"]
                    num_new = indict["num_waveforms"]

                    assert np.all(outdict["freqs"] == indict["freqs"])
                    outdict["psd"] = (num_accum * outdict["psd"] + num_new * indict["psd"]) / (num_accum + num_new)
                    outdict["num_waveforms"] += num_new

    with open(outpath, 'wb') as outfile:
        pickle.dump(combined_psds, outfile)
                    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--infiles", action = "store", dest = "inpaths", nargs = "+")
    parser.add_argument("--outfile", action = "store", dest = "outpath")
    args = vars(parser.parse_args())

    combine_psds(**args)

