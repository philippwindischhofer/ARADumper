from ARAReader import Reader
import pandas as pd

def extract_events_from_run(base_dir, run, channels, selector = lambda header: True):
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
        event_dfs.append(event_df)

    run_df = pd.concat(event_dfs, ignore_index = True)
    return run_df
