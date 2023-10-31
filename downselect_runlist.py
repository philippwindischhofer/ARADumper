import os, argparse, datetime
import pandas as pd

def downselect_runlist(inpath, veto_path, outpath, year_to_select, buflen_to_select, only_night, only_day):
    df_all = pd.read_csv(inpath)
    runs_vetoed = set(pd.read_csv(veto_path)["run"])

    if only_night:
        def cutter(row):
            cur_dt = datetime.datetime.fromisoformat(row["readout_time"])
            return (cur_dt.year == year_to_select) and (cur_dt.month >= 5) and (cur_dt.month <= 8) and (row["buffer_length"] == buflen_to_select)        
    elif only_day:
        raise RuntimeError("Error: not implemented yet")
    else:
        cutter = lambda row: (datetime.datetime.fromisoformat(row["readout_time"]).year == year_to_select) and (row["buffer_length"] == buflen_to_select)

    def cut_and_veto(row):
        return cutter(row) and (row["run"] not in runs_vetoed)
        
    df_selected = df_all[df_all.apply(cut_and_veto, axis = 1)]
    df_selected = df_selected.sort_values(by = "run", ascending = True)
    df_selected.to_csv(outpath, index = False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--all_runs", action = "store", dest = "inpath")
    parser.add_argument("--vetoed_runs", action = "store", dest = "veto_path")
    parser.add_argument("--out", action = "store", dest = "outpath")
    parser.add_argument("--year", action = "store", dest = "year_to_select", type = int, default = 2019)
    parser.add_argument("--night", action = "store_true", dest = "only_night", default = False)
    parser.add_argument("--day", action = "store_true", dest = "only_day", default = False)
    parser.add_argument("--buflen", action = "store", dest = "buflen_to_select", type = int, default = 512)
    args = vars(parser.parse_args())

    downselect_runlist(**args)
