import os, argparse, datetime
import pandas as pd

def downselect_runlist(inpath, veto_path, outpath, year_to_select):
    df_all = pd.read_csv(inpath)
    runs_vetoed = set(pd.read_csv(veto_path)["run"])

    year_selector = df_all.apply(lambda row: datetime.datetime.fromisoformat(row["readout_time"]).year == year_to_select, axis = 1)
    runs_in_year = set(df_all[year_selector]["run"])

    runs_selected = list(runs_in_year.symmetric_difference(runs_vetoed))
    df_selected = pd.DataFrame({"run": runs_selected})
    df_selected.to_csv(outpath, index = False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--all_runs", action = "store", dest = "inpath")
    parser.add_argument("--vetoed_runs", action = "store", dest = "veto_path")
    parser.add_argument("--out", action = "store", dest = "outpath")
    parser.add_argument("--year", action = "store", dest = "year_to_select", type = int, default = 2019)
    args = vars(parser.parse_args())

    downselect_runlist(**args)
