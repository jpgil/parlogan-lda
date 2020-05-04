from .config import *

def in_between(dataset, query_start, query_end, time_column = '@timestamp'):
    start_time = pd.to_datetime(dataset[time_column].min(), utc = True) - pd.Timedelta('10 minutes')
    end_time = pd.to_datetime(dataset[time_column].max(), utc = True) + pd.Timedelta('10 minutes')
    
    #get time ranges
    df_start = log_scan(query_start, start_time, end_time)
    df_start.rename(columns = {"@timestamp": time_column})
    df_start['event'] = 'start'
    if df_start.empty:
        return
    
    df_end = log_scan(query_end, start_time, end_time)
    df_end.rename(columns = {"@timestamp": time_column})
    df_end['event'] = 'end'
    if df_end.empty:
        return
    
    time_ranges = pd.concat([df_start, df_end],sort=False).sort_values(by = [time_column]).reset_index()
    time_ranges = time_ranges[time_ranges['event'] != time_ranges['event'].shift()]
    
    if time_ranges.iloc[0]['event'] == 'end':
        time_ranges.drop(0, inplace = True)
    
    if time_ranges.iloc[-1]['event'] == 'start':
        time_ranges.drop(time_ranges.tail(1).index, inplace = True)
    
    time_ranges = pd.DataFrame(time_ranges[time_column].values.reshape(-1, 2), columns = ['start', 'end'])
    
    mask = np.logical_or.reduce([
        (dataset[time_column] >= row.start) & (dataset[time_column] <= row.end)
        for _, row in time_ranges.iterrows()
    ])
    
    return dataset[mask]


def format_to_datetime(timestamp):
    """Takes date and turns it into a datetime.

    Args:
        timestamp (string, datetime, integer):  date to be converted.

    Returns:
        (datetime): date converted.

    """
    if isinstance(timestamp, int):
        return pd.to_datetime(timestamp, unit='ms', utc = True)
    return pd.to_datetime(timestamp, utc=True)



def apply_SELINS_FSM(SELINS_edges, system, no_end_delta = '30 minutes'):
    count = 0
    SELINS_exec = dict()
    started = False
    #get every SELINS execution
    for index, row in SELINS_edges.iterrows():
        #if the current log is a start
        if "MSW: Received command: SELINS, Buffer:" in row["logtext"]:
            #if already started, then there is no ending for the previous SELINS
            #finish the current one with a no end.
            if started:
                SELINS_exec[count]["end"] = SELINS_exec[count]["start"] + pd.Timedelta(no_end_delta)
                SELINS_exec[count]["no_end"] = True
                count += 1 
            #starting a new SELINS execution
            started = True
            SELINS_exec[count] = {
                "start": row["@timestamp"],
                "end": None,
                "system": row['system'],
                "mswERR_TARGET_TIMEOUT": False,
                "command_failed": False,
                "no_end": False,
            }
        #error -> end SELINS execution
        elif "mswERR_TARGET_TIMEOUT" in row["logtext"] and started:
            SELINS_exec[count]["end"] = row["@timestamp"]
            SELINS_exec[count]["mswERR_TARGET_TIMEOUT"] = True
            started = False
            count += 1

        #command failed -> end SELINS execution
        elif "Command failed (SELINS)" in row["logtext"] and started:
            SELINS_exec[count]["end"] = row["@timestamp"]
            SELINS_exec[count]["command_failed"] = True
            started = False
            count += 1

        #good ending found -> end SELINS execution
        elif "Telescope active optics calibrated correction" in row["logtext"] and started:
            SELINS_exec[count]["end"] = row["@timestamp"]
            started = False
            count += 1
            
    #checking if last execution ended
    if count in SELINS_exec and SELINS_exec[count]['end'] == None:
        del SELINS_exec[count]
    
    SELINS_exec = pd.DataFrame.from_dict(SELINS_exec).T
    SELINS_exec['success'] = ~(SELINS_exec["mswERR_TARGET_TIMEOUT"] | SELINS_exec["command_failed"])
    return SELINS_exec


def get_log_sequences(SELINS_exec, system, check_success = True):
    seqs = []
    for index, row in SELINS_exec.iterrows():
        #search all logs between start and end
        seq = log_scan(
            "%s AND (mswControl trkwsControl agwsControl procname: tif* /lt[1-4]...*/ TEL.ACTO* *ERR_* )"%system,
            row["start"],
            row["end"],
        ).sort_values(by = ["@timestamp"])
        
        #checking fixed points
        if not seq["logtext"].str.contains("Move M2 & M3").any() and check_success:
            continue
        if not seq["logtext"].str.contains("focus to STANDBY").any() and check_success:
            continue
        if not seq["logtext"].str.contains("focus to ONLINE").any() and check_success:
            continue 

        #timestamp to datetime
        seq["@timestamp"] = seq["@timestamp"].apply(format_to_datetime)

        #in case the query delivers wrong dates
        seq = seq[(seq["@timestamp"] >= row["start"]) & (seq["@timestamp"] <= row["end"])]

        #leaving the end at the end, might be other logs with the same timestamp
        #
        # This is part of the dataset generation, important!!!
        #
        #
        seq = pd.concat([seq[seq["logtext"] != "Telescope active optics calibrated correction"],
                               seq[seq["logtext"] == "Telescope active optics calibrated correction"]])
        

        #reseting indexes
        seq = seq.reset_index(drop=True)

        #adding the seq to the list of sequences
        seqs.append(seq)
    return seqs


def apply_color(seq, mode = 'standard'):
    seq['symbol_seq'] = (seq['envname'] +" "+
                         seq['procname'] +" "+
                         seq['logtext']).apply(lambda x: SELINS(x, mode))
    
    return seq[["@timestamp", "symbol_seq"]]


def get_SELINS_training_data(date, delta_time, system, no_end_time, min_mins, max_mins, mode = 'changeFocus'):
    end = pd.to_datetime(date, utc = True)
    start = end - pd.Timedelta(delta_time)
    
    query = '%s AND (NOT logtype:"FPAR") AND \
    ("MSW: Received command: SELINS, Buffer:" "Telescope active optics calibrated correction" \
    mswERR_TARGET_TIMEOUT "Command failed (SELINS)" )' % system
    
    SELINS_borders = log_scan(query, start, end).sort_values(by = ["@timestamp"])
    SELINS_borders["@timestamp"] = SELINS_borders["@timestamp"].apply(format_to_datetime)
    SELINS_exec = apply_SELINS_FSM(SELINS_borders, system, no_end_delta = no_end_time)
    SELINS_exec["minutes"] = (SELINS_exec['end'] - SELINS_exec['start']).astype('timedelta64[m]')
    SELINS_train = SELINS_exec[(SELINS_exec["minutes"] > min_mins) &
                               (SELINS_exec["minutes"] <= max_mins)&
                               (SELINS_exec["success"])]
    print('All SELINS Executions')
    display(SELINS_train)
    print(SELINS_train['system'].unique())
    log_seqs = get_log_sequences(SELINS_train, system)
    color_logseqs = [apply_color(seq, mode) for seq in log_seqs]
    
    if mode == 'changeFocus':
        color_logseqs = [seq for seq in color_logseqs
                          if seq["symbol_seq"].str.contains("from X Y").any()]
    elif mode == 'keepFocus':
        color_logseqs = [seq for seq in color_logseqs
                          if seq["symbol_seq"].str.contains("from X X").any()]
    

    x_train = pd.concat(color_logseqs, keys = range(len(color_logseqs)))
    x_train.index.set_names(["seq_id", "log_id"], inplace=True)
    return x_train
