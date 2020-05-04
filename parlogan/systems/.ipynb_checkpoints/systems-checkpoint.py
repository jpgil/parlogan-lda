from .config import *

def telescope_night(timestamp, telescope):
    timestamp = pd.to_datetime(timestamp)
    if timestamp.time() < time(12, 0):
        #we need to see the previous night
        end = datetime.combine(timestamp.date(), time(12, 0))
        start = end - pd.Timedelta('1 day')
    else:
        #we need to see the next night
        start = datetime.combine(timestamp.date(), time(12, 0))
        end = start + pd.Timedelta('1 day')
    print(start, end)
        
    query = f'{telescope} AND TEL.ENCL.SLITDOOR*'
    
    slitdoor = log_scan(query, start, end)
    
    if slitdoor.empty:
        return
    
    slitdoor.sort_values(by = '@timestamp', inplace = True)
    night_start = slitdoor[slitdoor['keywname'] == 'TEL.ENCL.SLITDOOR.OPEN']
    if night_start.empty:
        #there was no opening log
        night_start = start
    else:
        night_start = night_start.iloc[0]['@timestamp']
    
    night_end = slitdoor[
        (slitdoor['keywname'] == 'TEL.ENCL.SLITDOOR.CLOSE') &
        (slitdoor['@timestamp'] > night_start)
    ]
    if night_end.empty:
        #there was no closure
        night_end = end
    else:
        night_end = night_end.iloc[0]['@timestamp']
    return (night_start, night_end)

def instrument_night(timestamp, instr):
    telescope = telescope_by_instrument(timestamp, instr)
    return telescope_night(timestamp, telescope)


def telescope_by_instrument(timestamp, instr):
    end = pd.to_datetime(timestamp, utc = True)
    start = pd.to_datetime(timestamp, utc = True) - pd.Timedelta('30 days')
    query = f'system: (UT*) AND SELINS AND {instr}'
    SELINS = log_scan(query, start, end)
    
    if SELINS.empty:
        return
    
    SELINS.sort_values(by = '@timestamp', ascending = False, inplace = True)
    SELINS.reset_index(drop = True, inplace = True)
    return SELINS.iloc[0]['system']

def VLTi_telescop_type(timestamp):
    few_hours_ago = pd.to_datetime(timestamp) - pd.Timedelta(hours = 12)
    res = repr(log_scan('ISS.ARRAY.ARM', few_hours_ago, timestamp).sort_values("@timestamp").tail(1))
    if "AT" in res:
        return ["AT*", "NAOMI*", "DL RMNREC ARAL ISS"]
    elif "UT" in res:
        return ["UT*", "OP*", "CIAO*", "DL RMNREC ARAL ISS"]
    else:
        return []


def instrument_related_systems(timestamp, instr):
    #if the instrument is gravity, pionier or matisse
    if instr in VLTi_instruments:
        return VLTi_telescop_type(timestamp)
    #the rest
    return [telescope_by_instrument(timestamp, instr), ]


def related_system_errors(start, end, instr, delta = None):
    related_systems = instrument_related_systems(end, instr)
    related_systems = [instr, ] + related_systems
    all_sys_errors = dict()
    for related_sys in related_systems:
        try:
            if delta == None:
                errors = error_counting(
                    system = related_sys,
                    start = start,
                    end = end,
                    group_keys = ["errkey", "system"],
                    group_by_time = False
                )
            else:
                errors = error_counting(
                    system = related_sys,
                    start = start,
                    end = end,
                    group_keys = ["errkey", "system"],
                    delta = delta
                )
        except EmptyElasticQuery:
            continue

        if errors.empty:
            continue

        all_sys_errors[related_sys] = {
            sys: errors[errors['system'] == sys].sort_values(by = ["count", "errkey"], ascending = False)
            for sys in np.sort(errors['system'].unique())
        }

    return all_sys_errors
