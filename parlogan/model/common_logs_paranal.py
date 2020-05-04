# +
# This should go to a separate .py file
from .base_model_paranal import BaseModelPARANAL
from .common_logs import CommonLogs
from parlogan.db.es import log_scan
from parlogan import format_to_datetime
import pandas as pd

class CommonLogsPARANAL(BaseModelPARANAL, CommonLogs):
    
    def __init__(self, query, fixed_points, noise_tol = 1):
        BaseModelPARANAL.__init__(self)
        CommonLogs.__init__(self)
        self.query = query
        self.fixed_points = fixed_points
        self.noise_tol = noise_tol
    
    #########
    # Model #
    #########
    
    def save_model(self, filepath):
        CommonLogs.save_model(self, filepath)
    
    def load_model(self, filepath):
        CommonLogs.load_model(self, filepath)  
    
    ############
    # Training #
    ############
    
    def collect_training_dataset(self, start_ts, end_ts):
        """
        Return a pandas DataFrame with logs between start_ts and end_ts
        matching the query
        """
        start_ts = format_to_datetime(start_ts)
        end_ts = format_to_datetime(end_ts)
        
        dataset = log_scan(self.query, start_ts, end_ts)
        if not dataset.empty:
            dataset["@timestamp"] = dataset["@timestamp"].apply(format_to_datetime)
            return dataset.sort_values(by = ["@timestamp"])
        return dataset
    
    def fit(self, start_ts, end_ts):
        dataset = self.collect_training_dataset(start_ts, end_ts)
        if dataset.empty:
            return False
        CommonLogs.fit(self, dataset, self.fixed_points, self.noise_tol)
        print('Succesful Gym Session: fitter then ever!')
        return True
    
    ##############
    # Prediction #
    ##############
    
    def collect_prediction_dataset(self, timestamp):
        """
        Return a pandas DataFrame with logs arround timestamp matching the query.
        """
        timestamp = format_to_datetime(timestamp)
        start_ts = timestamp - pd.Timedelta('4 hours')
        end_ts = timestamp + pd.Timedelta('4 hours')
        
        dataset = log_scan(self.query, start_ts, end_ts)
        
        if not dataset.empty:
            dataset["@timestamp"] = dataset["@timestamp"].apply(format_to_datetime)
            return dataset.sort_values(by = ["@timestamp"])
        return dataset
    
    def predict(self, timestamp):
        dataset = self.collect_prediction_dataset(timestamp)
        if len(dataset) == 0:
            return None
        for selins in dataset:
            self.last_prediction = CommonLogs.predict(self, selins)
        return self.last_prediction
        
