import os, argparse, pickle
import numpy as np

def combine_correlators(outpath, inpaths):

    combined_correlators = {}

    for cur_inpath in inpaths:
        with open(cur_inpath, 'rb') as cur_infile:
            cur_data = pickle.load(cur_infile)
            
            for pairing_name, pairing_correlator in cur_data.items():
                if pairing_name not in combined_correlators:
                    combined_correlators[pairing_name] = {}                    

                indict = cur_data[pairing_name]
                outdict = combined_correlators[pairing_name]
                
                if "num_corr" not in outdict:
                    outdict["num_corr"] = indict["num_corr"]
                    outdict["mean"] = indict["mean"]
                    outdict["var"] = indict["var"]
                else:
                    num_accum = outdict["num_corr"]
                    num_new = indict["num_corr"]
                    
                    outdict["mean"] = (num_accum * outdict["mean"] + num_new * indict["mean"]) / (num_accum + num_new)
                    outdict["var"] = (num_accum * outdict["var"] + num_new * indict["var"]) / (num_accum + num_new)
                    outdict["num_corr"] += num_new

    with open(outpath, 'wb') as outfile:
        pickle.dump(combined_correlators, outfile)
                    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--infiles", action = "store", dest = "inpaths", nargs = "+")
    parser.add_argument("--outfile", action = "store", dest = "outpath")
    args = vars(parser.parse_args())

    combine_correlators(**args)
