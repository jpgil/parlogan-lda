from .common_logs_paranal import CommonLogsPARANAL
from parlogan import format_to_datetime
from parlogan.db.es import log_scan
from parlogan.color import PRESET
import pandas as pd
from parlogan import package_directory
from scipy import interpolate as spint
import datetime as dt
cl_preset_cache = package_directory + '/model/CommonLogsUTPRESETCache'

# +
class CommonLogsUTPRESET(CommonLogsPARANAL):
    def __init__(self, system, noise_tol = 1.):
        query = f'procname: prsControl AND system: {system} AND \
        ("PRESET: Succesfully completed. Command: SETUP, Buffer:" \
        "PRESET: Received command: SETUP, Buffer:") AND (TEL.TARG.ALPHA TEL.TARG.NAME TEL.TARG.EPOCHSYSTEM)'
        
        fixed_points = ['PRESET SETUP', 'PRESET SUCCESS']
        
        super().__init__(query, fixed_points, noise_tol)
        self.info['Model Name'] = 'CommonLogsUTPRESET'
        self.info['Description'] = 'Class for UTPRESET model'
        self.system = system
        
    #########
    # Model #
    #########
    
    def save_model(self, version):
        self.info['Model Version'] = version
        super().save_model(f"{cl_preset_cache}/models/{self.system}_{version}")
    
    def load_model(self, version):
        self.info['Model Version'] = version
        super().load_model(f"{cl_preset_cache}/models/{self.system}_{version}")
    
    def list_models(self):
        return super().list_models(f"{cl_preset_cache}/models/")
    
    def list_models():
        return CommonLogsPARANAL.list_models(f"{cl_preset_cache}/models/")
    
    ###########
    # Dataset #
    ###########
    
    def get_PRESET_logs(self, PRESETS_df):
        all_PRESET_logs = []
        for index, row in PRESETS_df.iterrows():
            PRESET_logs = log_scan(
                f"{self.system} (envname: (*alt* *az*) msw* agws* procname: (*ws* actcon* admain*))",
                row["start"],
                row["end"],
            )
            PRESET_logs['@timestamp'] = PRESET_logs['@timestamp'].apply(format_to_datetime)
            PRESET_logs.sort_values(by = ['@timestamp'], inplace = True)
            PRESET_logs.reset_index(drop = True, inplace = True)

            PRESET_logs = PRESET_logs[(PRESET_logs["@timestamp"] >= row["start"]) &
                                      (PRESET_logs["@timestamp"] <= row["end"])]

            start_index = PRESET_logs.index[
                PRESET_logs['logtext'].str.contains('PRESET: Received command: SETUP')
            ].tolist()[0]
            
            #check that the PRESET start is at index 0
            if start_index != 0:
                #swap with first
                curr_first, start = PRESET_logs.iloc[0].copy(), PRESET_logs.iloc[start_index].copy()
                PRESET_logs.iloc[0], PRESET_logs.iloc[start_index] = start, curr_first
            
            
            end_index = PRESET_logs.index[
                PRESET_logs['logtext'].str.contains('PRESET: Succesfully completed')
            ].tolist()
            
            #might have no end uwu
            if end_index != []:
                end_index = end_index[0]
                #check PRESET end is at index -1
                if end_index != PRESET_logs.shape[0]-1:
                    curr_last, end = PRESET_logs.iloc[-1].copy(), PRESET_logs.iloc[end_index].copy()
                    PRESET_logs.iloc[-1], PRESET_logs.iloc[end_index] = end, curr_last

            PRESET_logs.reset_index(drop = True, inplace = True)

            all_PRESET_logs.append(PRESET_logs)
        return all_PRESET_logs
    
    def apply_color_PRESET(self, PRESET_logs):
        PRESET_logs['symbol_seq'] = PRESET_logs['logtext'].apply(PRESET)
        return PRESET_logs[["@timestamp", "symbol_seq"]]
    
    def collect_PRESETS(self, events, include_no_ends = False):
        if events.empty:
            return events
        events = events[['@timestamp', 'logtext']]
        
        ends = events[events['logtext'].str.contains('Succesfully completed')].reset_index(drop=True)
        starts =  events[events['logtext'].str.contains('Received command')].reset_index(drop=True)
        
        if ends.empty or starts.empty:
            return pd.DataFrame()
        
        starts.columns = ['start', 'buffer_start']
        ends.columns = ['end', 'buffer_end']
        
        starts['buffer_start'] = starts.buffer_start.map(
            lambda x: x.replace('PRESET: Received command: SETUP, Buffer: ', '')
        )
        ends['buffer_end'] = ends.buffer_end.map(
            lambda x: x.replace('PRESET: Succesfully completed. Command: SETUP, Buffer: ', '')
        )
        
        interpolate = spint.interp1d(
            ends.end.values.astype(float), ends.end.values.astype(float),
            kind='next', assume_sorted=True, fill_value='extrapolate'
        )
        starts['end'] = pd.to_datetime(interpolate(starts.start.values.astype(float)), utc=True).round('ms')
        mask = starts['end'].shift(-1) == starts['end']

        good_ends = starts[~mask]
        data = pd.merge(good_ends, ends, on='end')
        data = data[data.buffer_start == data.buffer_end].drop(columns=['buffer_start', 'buffer_end'])
        data['elapsed'] = data.end - data.start
        data = data[data.elapsed > dt.timedelta(seconds=5)].reset_index(drop = True)
        
        no_ends = starts[mask]
        
        if include_no_ends and not no_ends.empty:
            no_ends['end'] = pd.concat([
                starts.shift(-1)['start'][mask], no_ends['start'] + pd.Timedelta('15 minutes')
            ], axis = 1).min(axis = 1)
            
            
            no_ends.drop(columns = ['buffer_start'], inplace = True)
            no_ends['elapsed'] = no_ends.end - no_ends.start

            all_presets = pd.concat([no_ends, data]).reset_index(drop = True)
            return all_presets
        
        return data
        
#         if len(starts.index)==1:
#             ends['start'] = pd.to_datetime(starts.start.values.astype(float)).round('ms')
#         else:
#             interpolate = spint.interp1d(
#                 starts.start.values.astype(float), starts.start.values.astype(float),
#                 kind='previous', assume_sorted=True, fill_value='extrapolate'
#             )
#             ends['start'] = pd.to_datetime(interpolate(ends.end.values.astype(float)), utc=True).round('ms')

#         data = pd.merge(starts, ends, on='start')
#         data = data[data.buffer_start == data.buffer_end].drop(columns=['buffer_start', 'buffer_end'])
#         if data.empty:
#             return data
#         data['elapsed'] = data.end - data.start
#         data = data[data.elapsed > dt.timedelta(seconds=5)].reset_index(drop=True)
#         return data
        
    
    def collect_training_dataset(self, start_ts, end_ts):
        events = super().collect_training_dataset(start_ts, end_ts)
        PRESETS = self.collect_PRESETS(events)
        
        if PRESETS.empty:
            return PRESETS
        
        all_PRESET_logs = self.get_PRESET_logs(PRESETS)
        color_PRESETS = [self.apply_color_PRESET(PRESET_logs) for PRESET_logs in all_PRESET_logs]
        print(f'Training with {len(color_PRESETS)} examples :D')
        PRESET_train = pd.concat(color_PRESETS, keys = range(len(color_PRESETS)))
        PRESET_train.index.set_names(["seq_id", "log_id"], inplace=True)
        return PRESET_train
    
    def collect_prediction_dataset(self, timestamp):
        events = super().collect_prediction_dataset(timestamp).reset_index(drop = True)
        PRESETS = self.collect_PRESETS(events, include_no_ends = True)
        if PRESETS.empty:
            return PRESETS
        PRESETS = PRESETS[(PRESETS['start'] <= timestamp) & (PRESETS['end'] >= timestamp)]
        if PRESETS.empty:
            return PRESETS
        all_PRESET_logs = self.get_PRESET_logs(PRESETS)
        color_PRESETS = [self.apply_color_PRESET(PRESET_logs) for PRESET_logs in all_PRESET_logs]
        return color_PRESETS
        
#         execs = self.apply_SELINS_FSM(events)
#         if execs.empty:
#             return
            
#         timestamp = format_to_datetime(timestamp)
#         execs = execs[(execs['start'] <= timestamp) & (execs['end'] >= timestamp)]
        
#         if execs.empty:
#             return
            
#         execs["minutes"] = (execs['end'] - execs['start']).astype('timedelta64[m]')
#         seqs = self.get_log_sequences(execs, check_success = False)
#         seqs = [self.apply_color(seq, 'standard') for seq in seqs]
        
#         return seqs
        
