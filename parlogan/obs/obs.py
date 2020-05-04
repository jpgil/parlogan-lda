from .config import *

def OBs_between(system, start, end):
    """Get OBs between two timestamps of one instrument

    Args:
        system (string): instrument
        start (timestamp)
        end (timestamp)

    Returns:
        (DataFrame): OBs found
    """
    
    #name of the dataset
    file_name = raw_data_folder + f'OBsBetween_{system}_{start.isoformat()}_{end.isoformat()}.csv'
    raw_obs_logs = pd.DataFrame()
    
    #if os.path.exists(file_name):
    #    raw_obs_logs = pd.read_csv(file_name)
        
    if raw_obs_logs.empty:
        raw_obs_logs = log_scan('(%s) AND logtext: ((OBS.NAME AND OBS.ID) \
            "OB started at" "OB finished" "OB aborted" "OB paused" "OB continued" "ACK ABORT (red)")' % system,
            start, end)
        raw_obs_logs.to_csv(file_name)
    
    #if empty, return none
    if raw_obs_logs.empty:
        return raw_obs_logs
    
    #leaving only the relevant cols and sorting
    raw_obs_logs = raw_obs_logs[[
        "@timestamp", "logtype", "system", "loghost", "hostname", "envname", "procname",
        "procid", "module", "logtext"]].sort_values("@timestamp")
    
    obs = parse_observations(
        raw_obs_logs,
        get_OB_FSM()
    )
#     with pd.option_context(
#         'display.max_rows', None,
#         'display.max_columns', None,
#         'display.max_colwidth', -1):
#         display(obs)
    
    if obs.empty:
        return obs
    
    obs.sort_values(by='START', ascending = True, inplace = True)
    obs["End_with_error"] = (obs["Aborted"] & ~obs["ACK ABORT"])
    
    #all timestamps to datetimes
    obs['START'] = pd.to_datetime(obs['START'], utc = True)
    obs['END'] = pd.to_datetime(obs['END'], utc = True)
    
    return obs

def get_OB_FSM():
    """OB State Machine"""
    H = nx.DiGraph()
    H.add_edges_from([
        ("_Start_", "START"),
        ("START", "STOP"),
        ("START", "ABORT"),
        ("START", "(Manual Abort)"),
        ("START", "START"),
        ("STOP", "START"),
        ("STOP", "_End_"),
        ("(Manual Abort)", "ABORT"),
        ("ABORT", "START"),
        ("ABORT", "_End_")
    ])
    # Add query filter
    H.nodes["START"]['filter'] = "OB started at"
    H.nodes["STOP"]['filter'] = "OB finished"
    H.nodes["(Manual Abort)"]['filter'] = "ACK ABORT (red)"
    H.nodes["ABORT"]['filter'] = "OB aborted after"
    
    return H


def parse_observations(logs, H):
    """Uses OB state machine to get OBs (start, end)

    Args:
        logs (DataFrame)
        H (DiGraph): OB State machine 

    Returns:
        (DataFrame): OBs found
    """
    H.state = "_Start_"

    obs_dates = []
    def clear_current_obs():
        return {
            'pauses': 0,
            'OBS.NAME': 'unknown',
            'OBS.ID': 'unknown',
            'ACK ABORT': False,
            'Aborted': False
    }
    
    current_obs = clear_current_obs()
    already_started = False
    for index, row in logs.iterrows():
        logtext = row['logtext']

        # INFO collectors
        # (OBS.NAME: 7-Cet // OBS.ID: 200454825)
        if '(OBS.NAME' in logtext:
            current_obs['OBS.NAME'] = logtext.split("//")[0].split(":")[1].strip() # 7-Cet
            current_obs['OBS.ID'] = logtext.split(":")[2][:-1].strip() # 200454825
            
        elif 'OB paused at' in logtext:
            current_obs['pauses'] += 1
            
        elif 'ACK ABORT' in logtext:
            current_obs['ACK ABORT'] = True
        else:
        #print(row['@timestamp'], row['system'], row['procname'], row['logtext'])
            pass
        
        NS = [ b for (a,b) in H.edges(H.state)
            if 'filter' in H.nodes[b].keys()
            and H.nodes[b]['filter'] in logtext ]

        # I have several goal states, this shouldn't happen!
        if len(NS) > 1:
            print("WARNING: A state was already found from this transition")
            print(H.state, NS)
            sys.exit()
            
        # I have only and only one state.
        elif len(NS) == 1:
            #print(">> New state found: %s -> %s" % (H.state, b) )
            H.state = NS[0]
            # On Change, I will add this info to current_obs
            if H.state == "START":
                if already_started:
                    #there is no finish log
                    current_obs['END'] = (pd.to_datetime(row["@timestamp"], utc = True) - pd.Timedelta('1 second')).isoformat()
                    current_obs["Seconds"] = (pd.to_datetime(current_obs['END'], utc = True) - pd.to_datetime(current_obs['START'], utc = True)).total_seconds()
                    current_obs['Aborted'] = True
                    
                    obs_dates.append(dict(current_obs))
                    current_obs = clear_current_obs()
                    
                current_obs['START'] = row["@timestamp"]
                already_started = True
            elif H.state == "(Manual Abort)":
                current_obs['ACK ABORT'] = True

            # OB finished (TERMINATED) in 4800 seconds at 2018-12-15T21:58:56
            elif H.state == "STOP":
                current_obs['END'] = row["@timestamp"]
                current_obs["Seconds"] = int(logtext.split()[4])
                # Consistency Check:
                #if logtext.split()[7] != current_obs['START']:
                # print("WARNING! ", current_obs['START'], logtext)
                #( pd.to_datetime( a.split()[6] ) - pd.to_datetime("1970-01-01") ).total_seconds()

            # OB aborted after 1669 seconds at 2018-12-01T20:16:03
            elif H.state == "ABORT":
                current_obs['END'] = row["@timestamp"]
                current_obs["Seconds"] = int(logtext.split()[3])
                current_obs['Aborted'] = True
                
            if H.state in ["ABORT", "STOP"]:
                obs_dates.append(dict(current_obs))
                current_obs = clear_current_obs()
                already_started = False

    return pd.DataFrame.from_records(obs_dates)


def OB_status(OB):
    """if OB is success, aborted or manual abort"""
    if OB['ACK ABORT'] and OB['Aborted']:
        return 'Manual Abort'
    elif OB['Aborted']:
        return 'Failed'
    return 'Success'


def sort_yellows(OB_logs):
    '''Some logs have the same timestamp, making hard to order. yellows logs have to appear
    before a started log
    '''
    yellows = OB_logs['logtext'].str.extract(r'(.*) \(yellow\)').dropna()
    
    for i in yellows.index.values:
        if 'Started at' in OB_logs['logtext'].iloc[i-1]:
            #swap rows
            curr, prev = OB_logs.iloc[i].copy(), OB_logs.iloc[i-1].copy()
            OB_logs.iloc[i], OB_logs.iloc[i-1] = prev, curr
    OB_logs = OB_logs.reset_index(drop=True)
    return OB_logs


def OB_logs_between(start, end, system):
    """OB logs between two timestamps

    Args:
        start (timestamp): start of OB
        end (timestamp): end of OB
        system (string): instrument
        

    Returns:
        (DataFrame) logs
    """
    
    #making sure that start and end are Timestamps
    start = pd.to_datetime(start, utc = True)
    end = pd.to_datetime(end, utc = True)
    
    #csv might exists with the data needed
    filename = raw_data_folder + f"OBDescribe_{system}_{start.isoformat()}_{end.isoformat()}"
    
    if os.path.exists(filename):
        OB_logs = pd.read_csv(filename)
    else:
        query = f'system: {system} AND ((bob*)(keywname: OBS.*))'
        OB_logs = log_scan(query, start, end)
        OB_logs.to_csv(filename)
    
    return OB_logs


def get_OB_name_id(start, end, system):
    """get name and id of OB

    Args:
        start (timestamp): start of OB
        end (timestamp): end of OB
        system (string): instrument

    Returns:
        (list[string], list[string]) name and id
    """
    hits = log_scan(f'system:{system} AND logtext: OBS.NAME AND logtext: OBS.ID', start, end)
    name_and_id = hits["logtext"].str.extract(r'\(OBS.NAME: (?P<name>.*) // OBS.ID: (?P<id>.*)\)').dropna()
    return (name_and_id['name'].iloc[0], name_and_id['id'].iloc[0])


def get_OB_proc_name_id(OB_logs):
    """get procname and procid of OB

    Args:
        OB_logs (DataFrame): logs of OB

    Returns:
        (list[string], list[string]) procname and procid
    """
    bob_procnames = OB_logs[OB_logs['procname'].str.contains('bob_')]
    return (bob_procnames['procname'].unique(), bob_procnames['procid'].unique())


def get_OB_kibana_url(start, end, system):
    """get url to see OB logs in Kibana

    Args:
        start (timestamp): start of OB
        end (timestamp): end of OB
        system (string): instrument

    Returns:
        (string) kibana url
    """
    #making sure that start and end are Timestamps
    start = pd.to_datetime(start, utc = True)
    end = pd.to_datetime(end, utc = True)
    
    return kibana_format % (
        start.isoformat().replace('+00:00', 'Z'),
        end.isoformat().replace('+00:00', 'Z'),
        f'{system}%20AND%20procname:%20bob_*'
    )


def get_OB_errors(OB_logs):
    """Get errors in instrument during OB

    Args:
        OB_logs (DataFrame): logs during OB

    Returns:
        (DataFrame) Errors in instrument 
    """
    return OB_logs[OB_logs["logtext"].str.contains("ERR_")]


def get_surrounding_OBs(OB_start, OB_end, system):
    """Get previous OB and next OB of current OB

    Args:
        OB_start (timestamp)
        OB_end (timestamp)
        system (string): instrument

    Returns:
        (DataFrame) Previous and Next OB
    """
    #It is performed a query with 14 days, 7 before the date, 7 after the date
    start = pd.to_datetime(OB_start, utc = True) - pd.Timedelta("7 days")
    end = pd.to_datetime(OB_end, utc = True) + pd.Timedelta("7 days")
    
    #now we have our current OB and the surrounding ones in this DF
    all_OBs = OBs_between(system, start, end)
    
    prev_OB = all_OBs[all_OBs["START"] < OB_start].tail(1)
    next_OB = all_OBs[all_OBs["END"] > OB_end].head(1)
    
    prev_next = pd.concat([prev_OB, next_OB])
    
    if not prev_OB.empty and not next_OB.empty:
        indexes = pd.Index(["Previous", "Next"])
    elif prev_OB.empty and next_OB.empty:
        return pd.DataFrame()
    elif prev_OB.empty:
        indexes = pd.Index(["Next"])
    else:
        indexes = pd.Index(["Prev"])
    
    prev_next.set_index([indexes], inplace = True)
    prev_next['STATUS'] = prev_next.apply(OB_status, axis = 1)
    return prev_next[['STATUS', 'OBS.NAME', 'OBS.ID', 'pauses', 'Seconds', 'START', 'END']]


def get_OB_params(OB_logs):
    """Get parameters set in OB

    Args:
        OB_logs (DataFrame): logs in OB

    Returns:
        (DataFrame) Parameters set in OB
    """
    #logs, params, and no params
    OB_params = pd.concat(
        [
                OB_logs['logtext'].str.extract(params_regex),
                OB_logs['@timestamp']
            ],
            axis = 1,
            sort = False
        ).dropna()
    
    OB_params['value'] = OB_params['value'].astype(str).str.strip("'")
    OB_params_cols = OB_params['params'].str.replace('.', ' ').str.split(' ', expand=True)
    OB_params_split = pd.concat([OB_params_cols, OB_params.drop(columns = ['params'])], axis=1, sort=False)
    return OB_params_split.reset_index(drop = True).sort_values(by = '@timestamp').fillna('')


def get_OB_logs_no_params(OB_logs):
    """Get logs that not set parameters in OB

    Args:
        OB_logs (DataFrame): logs in OB

    Returns:
        (DataFrame) Logs in OB
    """
    return OB_logs[~OB_logs['logtext'].str.contains(params_regex_wogroups)]


def get_OB_blocks(OB_logs):
    """Get observation blocks in OB

    Args:
        OB_logs (DataFrame): logs in OB

    Returns:
        (list[DataFrame], list[string]) blocks and name of this blocks
    """
    #getting block titles
    OB_logs["yellow"] = OB_logs["logtext"].str.extract(r'(.*) \(yellow\)')
    block_names = OB_logs.dropna().drop(columns = ['logtext'])
    
    #block titles indexes
    block_indexes = list(block_names.index.values)

    bi_mod = block_indexes + [OB_logs.shape[0]-1]
    
    #partition
    blocks = [OB_logs.iloc[bi_mod[n]:bi_mod[n+1]] for n in range(len(bi_mod)-1)]
    return blocks, block_names['yellow'].to_list()


def get_block_params(block, max_levels = 2):
    """Get parameters set in an observation block

    Args:
        block (DataFrame): logs during an observation block

    Returns:
        (DataFrame) Parameters set in block.
    """
    block_params = pd.concat(
        [block['logtext'].str.extract(params_regex),block['@timestamp']],
        axis = 1,
        sort = False
    ).dropna()
    
    if block_params.empty:
        return block_params
    
    block_params['value'] = block_params['value'].astype(str).str.strip("'")
    block_params_cols = block_params['params'].str.replace('.', ' ').str.split(' ', n = max_levels, expand=True)
    if max_levels in block_params_cols:
        block_params_cols[max_levels] = block_params_cols[max_levels].str.replace(' ', '.')
    block_params_split = pd.concat([
        block_params_cols, block_params.drop(columns = ['params'])
    ], axis=1, sort=False)

    block_params_split.set_index(
        list(block_params_cols.columns),
        inplace = True
    )
    block_params_split.sort_values(
        by = list(block_params_cols.columns) + ['@timestamp',],
        inplace = True
    )
    
    block_params_split.index.set_names([
        f'param{i}' for i in range(len(block_params_split.index.names))
    ],inplace = True)
    
    return block_params_split


def get_block_logs_no_params(block):
    """Get logs that not set parameters in an observation block

    Args:
        block (DataFrame): logs during an observation block

    Returns:
        (DataFrame) Logs in block.
    """
    return block[
        (~block['logtext'].str.contains(params_regex_wogroups)) & (block["yellow"].isna())
    ][['@timestamp', 'logtext']]


def OB_describe(OB, system, prev_next = True, bobby_mode = True):
    """Get all the info to show in the OB Context Tool or Bobby

    Args:
        OB (Series): observation
        system (string): instrument
        prev_next (bool): if to get surrounding OBs
        bobby_mode (bool): if getting info for bobby is needed

    Returns:
        (dict) Dictionary with all the info of the OB. Keys:
            start (timestamp)
            end (timestamp)
            seconds (int)
            is_bad_ending (bool)
            status (string)
            name (string)
            id (int)
            procnames (list)
            procids (list)
            kibana_link (string)
            errors (DataFrame)
            system_errors (dict(system (string): errors (DataFrame))),
            prev_next (DataFrame)
            params (DataFrame)
            logs (DataFrame)
            no_params (DataFrame)
            
        if bobby_mode is true, then we have additional keys:
            blocks (list[DataFrame])
            block_names (list[strings])
            block_params (list[DataFrame])
            block_logs (list[DataFrame])
            block_params (list[DataFrame])
            block_logs (list[DataFrame])
    """
    OB_info = dict()
    #getting general info
    OB_info['start'] = OB["START"]
    OB_info['end'] = OB["END"]
    OB_info['seconds'] = OB['Seconds']
    OB_info['is_bad_ending'] = OB["End_with_error"]
    OB_info['status'] = OB_status(OB)
    
    #getting all logs of this OB
    OB_logs = OB_logs_between(OB_info['start'], OB_info['end'], system)
    
    #if we are in bobby mode, then we just need procname = bob_something
    if bobby_mode:
        OB_logs = OB_logs[OB_logs.procname.str.contains('bob')]
    
    
    OB_logs['@timestamp'] = OB_logs['@timestamp'].apply(format_to_datetime)
    OB_logs = OB_logs.sort_values("@timestamp").reset_index(drop=True)
    
    #using just the columns that are needed
    OB_logs = OB_logs[["@timestamp", "logtype", "system", "loghost", "hostname", "envname", "procname",
                       "procid", "module", "logtext"]]
    
    #some logs have the same timestamp and need a special order
    OB_logs = sort_yellows(OB_logs)
    
    #name and id
    OB_info['name'], OB_info['id'] = get_OB_name_id(OB["START"], OB["END"], system)
    
    #check if OB name and id are valid
    if (OB["OBS.NAME"] != OB_info['name']) and (OB["OBS.ID"] != OB_info['id']):
        return dict()
    
    #procnames and procids
    OB_info['procnames'], OB_info['procids'] = get_OB_proc_name_id(OB_logs)
    
    #kibana
    OB_info['kibana_link'] = get_OB_kibana_url(OB_info['start'], OB_info['end'], system)
    
    #errors
    OB_info['errors'] = get_OB_errors(OB_logs)
    
    #error counting per telescope
    OB_info['system_errors'] = related_system_errors(OB_info['start'], OB_info['end'], system)
    
    #prev and next OB
    if prev_next:
        OB_info['prev_next'] = get_surrounding_OBs(OB_info['start'], OB_info['end'], system)
    
    #logs, params, and no params
    OB_info['params'] = get_OB_params(OB_logs)
    OB_info['logs'] = OB_logs
    OB_info['no_params'] = get_OB_logs_no_params(OB_logs) 
    
    #all for bobby
    if bobby_mode:
        OB_info['blocks'], OB_info['block_names'] = get_OB_blocks(OB_logs)
        #params and logs for blocks
        OB_info['block_params'] = []
        OB_info['block_logs'] = []
        for block in OB_info['blocks']:
            OB_info['block_params'].append(get_block_params(block))
            OB_info['block_logs'].append(get_block_logs_no_params(block))

    return OB_info
