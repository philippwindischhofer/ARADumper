import os, argparse, datetime
import pandas as pd

def downselect_runlist(inpath, veto_path, outpath, year_to_select, buflen_to_select):
    df_all = pd.read_csv(inpath)
    runs_vetoed = set(pd.read_csv(veto_path)["run"])

    selector = df_all.apply(lambda row: datetime.datetime.fromisoformat(row["readout_time"]).year == year_to_select and row["buffer_length"] == buflen_to_select,
                            axis = 1)
    runs_selected = set(df_all[selector]["run"])
    runs_selected = list(runs_selected.difference(runs_vetoed))
    
    df_selected = pd.DataFrame({"run": runs_selected})
    df_selected.to_csv(outpath, index = False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--all_runs", action = "store", dest = "inpath")
    parser.add_argument("--vetoed_runs", action = "store", dest = "veto_path")
    parser.add_argument("--out", action = "store", dest = "outpath")
    parser.add_argument("--year", action = "store", dest = "year_to_select", type = int, default = 2019)
    parser.add_argument("--buflen", action = "store", dest = "buflen_to_select", type = int, default = 512)
    args = vars(parser.parse_args())

    downselect_runlist(**args)
