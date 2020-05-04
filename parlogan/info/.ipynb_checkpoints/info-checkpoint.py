import pandas as pd
pd.options.mode.chained_assignment = None
from parlogan.db.es import log_scan, EmptyElasticQuery, Error
from parlogan import format_to_datetime

def error_counting(system = '*', envname = '*', hostname = '*', loghost = '*', module = '*',
                   filter_query = None, date = pd.datetime.now(), time_back = '20 minutes',
                   delta = '5 min', start = None, end = None, 
                   group_keys = ["errkey", "system", "envname", "hostname", "loghost", "module"],
                   group_by_time = True):
    """Calculates error frequency given the keys and deltatimes to group errors.

    Args:
        system (string): system where to look for errors. All by default.
        envname (string): envname where to look for errors. All by default.
        hostname (string): hostname where to look for errors. All by default.
        loghost (string): loghost where to look for errors. All by default.
        module (string): module where to look for errors. All by default.
        filter_query (string): kibana query. If it is set, all arguments above are ignored.
        date (timestamp): datetime where to look back for errors.
        time_back (string): how much time to look back for errors.
        delta (string): every how much time errors are counted.
        start (timestamp): where to start to look for errors.
        end (timestamp): where to stop looking for errors.
        group_keys (list[string]): keys of logs to group errors for
        group_by_time (bool): if to group by time (with the delta argument) or not

    Returns:
        (DataFrame): Error Counting
    """
    
    #Check if a start and end is given
    if start == None and end == None:
        end = pd.to_datetime(date, utc=True)    
        start = end - pd.Timedelta(time_back)
        
    elif start == None:
        print("Must specify start and end, if not, a date and a time back.")
        return
    else:
        end = pd.to_datetime(end, utc=True)
        start = pd.to_datetime(start, utc=True)
    
    #query
    if not filter_query:
        query = "*ERR_* AND system: (%s) AND envname: (%s) AND hostname: (%s) AND loghost: (%s)"\
                    " AND module: (%s)" % (system, envname, hostname, loghost, module)
    else:
        query = filter_query
        
    errors = log_scan(
        query,
        start,
        end,
    )
            
    if errors.empty:
        error_message = "\nQuery: " + query + "\nDate range: " + str(start) + " to " + str(end)
        raise EmptyElasticQuery(Error(error_message))
    
    errors["@timestamp"] = errors["@timestamp"].apply(format_to_datetime)
    errors = errors[(errors['@timestamp'] > start) & (errors['@timestamp'] <= end)]
    
    error_keys = errors.logtext.str.extract(r'(?P<errkey>[A-Z0-9]+[A-Z0-9/. ]*)')
    #splitted_logtext = errors.logtext.str.split(pat=":", n = 1, expand = True)
    #errors["errkey"] = splitted_logtext[0].str.strip()
    errors["errkey"] = errors.logtext.str.extract(r'(?P<errkey>[\w]*ERR_[^\s]*) : .*')
    errors.dropna(inplace = True)
    errors = errors.sort_values(by=["@timestamp", "errkey"])
    
    errors = errors[["@timestamp","system","envname","hostname","loghost","module","errkey"]]
    
    if group_by_time:
        counts = errors.groupby([
            pd.Grouper(key='@timestamp',freq = delta, base = end.minute, closed = "right", label = "right")
        ] + group_keys).size().reset_index(name='count')
    else:
        counts = errors.groupby(group_keys).size().reset_index(name='count')
    
    return counts
