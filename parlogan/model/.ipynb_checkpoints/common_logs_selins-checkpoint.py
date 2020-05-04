# +
from .common_logs_paranal import CommonLogsPARANAL
from parlogan import format_to_datetime
from parlogan.db.es import log_scan
from parlogan.color import SELINS
import pandas as pd
from parlogan import package_directory
cl_selins_cache = package_directory + '/model/CommonLogsSELINSCache'

class CommonLogsSELINS(CommonLogsPARANAL):
    
    def __init__(self, system, selins_mode, noise_tol = 1.):
        query = f'{system} AND \
("MSW: Received command: SELINS, Buffer:" "Telescope active optics calibrated correction" \
mswERR_TARGET_TIMEOUT "Command failed (SELINS)" )'
        
        if selins_mode == 'ChangeFocus':
            fixed_points = [
                "wtXtcs mswControl MSW Received command SELINS Buffer",
                "wtXtcs mswControl CHANGE FOCUS Move M2 M3 from X Y",
                "wtXtcs mswControl CHANGE FOCUS Setting all modules on new Y focus ONLINE",
                "wtXtcs logManager Telescope active optics calibrated correction",
            ]
        else:
            fixed_points = [
                "wtXtcs mswControl MSW Received command SELINS Buffer",
                "wtXtcs mswControl CHANGE FOCUS Move M2 M3 from X X",
                "wtXtcs mswControl CHANGE FOCUS Setting all modules on new Y focus ONLINE",
                "wtXtcs logManager Telescope active optics calibrated correction",
            ]
        super().__init__(query, fixed_points, noise_tol)
        self.selins_mode = selins_mode
        self.info['Model Name'] = 'CommonLogsSELINS'
        self.info['Model Version'] = '0.0.0' #gotta calculate or take from user
        self.info['Description'] = 'Class for SELINS model.'
        self.system = system
    
    #########
    # Model #
    #########
    
    def save_model(self, version):
        self.info['Model Version'] = version
        super().save_model(f"{cl_selins_cache}/models/{self.system}_{self.selins_mode}_{version}")
    
    def load_model(self, version):
        self.info['Model Version'] = version
        super().load_model(f"{cl_selins_cache}/models/{self.system}_{self.selins_mode}_{version}")
    
    def list_models(self):
        return super().list_models(f"{cl_selins_cache}/models/")
    
    def list_models():
        return CommonLogsPARANAL.list_models(f"{cl_selins_cache}/models/")
    
    ###########
    # Dataset #
    ###########
    
    def apply_color(self, seq, mode = 'standard'):
        seq['symbol_seq'] = (seq['envname'] +" "+
                             seq['procname'] +" "+
                             seq['logtext']).apply(lambda x: SELINS(x, mode))

        return seq[["@timestamp", "symbol_seq"]]

    def get_log_sequences(self, SELINS_exec, check_success = True):
        seqs = []
        for index, row in SELINS_exec.iterrows():
            #search all logs between start and end
            system = row['system']
            seq = log_scan(
                f"{system} AND (mswControl trkwsControl agwsControl procname: tif* /lt[1-4]...*/ TEL.ACTO* *ERR_* )",
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

            seq = seq[(seq["@timestamp"] >= row["start"]) & (seq["@timestamp"] <= row["end"])]
            seq = pd.concat([seq[seq["logtext"] != "Telescope active optics calibrated correction"],
                                   seq[seq["logtext"] == "Telescope active optics calibrated correction"]])


            #reseting indexes
            seq = seq.reset_index(drop=True)

            #adding the seq to the list of sequences
            seqs.append(seq)
        return seqs
    
    def apply_SELINS_FSM(self, SELINS_edges, no_end_delta = '30 minutes'):
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
        if SELINS_exec.empty:
            return SELINS_exec
        
        SELINS_exec['success'] = ~(SELINS_exec["mswERR_TARGET_TIMEOUT"] | SELINS_exec["command_failed"])
        return SELINS_exec
    
    def collect_training_dataset(self, start_ts, end_ts):
        events = super().collect_training_dataset(start_ts, end_ts).reset_index(drop = True)
        if events.empty:
            return pd.DataFrame()
        execs = self.apply_SELINS_FSM(events)
        if execs.empty:
            return pd.DataFrame()
        execs["minutes"] = (execs['end'] - execs['start']).astype('timedelta64[m]')
        seqs = self.get_log_sequences(execs)
    
        if self.selins_mode == 'ChangeFocus':
            seqs = [self.apply_color(seq, 'standard') for seq in seqs]
            seqs = [seq for seq in seqs if seq["symbol_seq"].str.contains("from X Y").any()]
        #elif self.selins_mode == 'keepFocus':
        else:
            seqs = [self.apply_color(seq, 'standard') for seq in seqs]
            seqs = [seq for seq in seqs if seq["symbol_seq"].str.contains("from X X").any()]
        
        x_train = pd.concat(seqs, keys = range(len(seqs)))
        x_train.index.set_names(["seq_id", "log_id"], inplace=True)
        print(f'Training with {len(seqs)} examples :D')
        return x_train
    
    def collect_prediction_dataset(self, timestamp):
        events = super().collect_prediction_dataset(timestamp).reset_index(drop = True)
        if events.empty:
            return []
        
        execs = self.apply_SELINS_FSM(events)
        if execs.empty:
            return []
            
        timestamp = format_to_datetime(timestamp)
        execs = execs[(execs['start'] <= timestamp) & (execs['end'] >= timestamp)]
        
        if execs.empty:
            return []
            
        execs["minutes"] = (execs['end'] - execs['start']).astype('timedelta64[m]')
        seqs = self.get_log_sequences(execs, check_success = False)
        seqs = [self.apply_color(seq, 'standard') for seq in seqs]
        
        return seqs
    
    
        
    
#     def collect_dataset_alt(self, system, start_event_keys, stop_event_keys, start, end):
#         start_events = super().collect_dataset(f'{system} AND ({start_event_keys})', start, end)
#         stop_events = super().collect_dataset(f'{system} AND ({stop_event_keys})', start, end)
#         events = pd.concat([start_events, stop_events]).reset_index(drop = True)
#         events["@timestamp"] = events["@timestamp"].apply(format_to_datetime)
#         execs = self.apply_SELINS_FSM(events)
        
#         return execs
    
    
    
