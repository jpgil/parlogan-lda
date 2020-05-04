# +
import networkx as nx
import pandas as pd
import glob
model_regex = r".*/(?P<model>.*).JSON"

class BaseModelPARANAL:
    """Parent class for all the models.

    Attributes:
        last_prediction: stores the last prediction of the model
        model: trained model
        info: dictionary with info about the class
    """
    
    ########
    # init #
    ########
    
    def __init__(self):
        self.last_prediction = None
        self.model = None
        self.info = {
            'Model Name'   : '',
            'Model Version': '0.0.0',
            'Description'  : 'This is the base class for all the models. Please overwrite me.', 
            'Last Trained' : None,
            'Train Dataset': "No local copies found"
        }

    #########################
    # Model related methods #
    #########################    

    def model_info(self):
        self.info['Model Name'] = self.__class__.__name__        
        return pd.DataFrame.from_dict( [self.info] ).T
    
    def explain_model(self):
        return "[%s] Insert a nice drawing of the trained model here." % self.__class__.__name__
    
    def save_model(self, filepath):
        return "[%s] Save model here!"
    
    def load_model(self, filepath):
        return "[%s] Load model here!"
    
    def list_models(self, folderpath):
        return glob.glob(f"{folderpath}/*.JSON")
    
    def list_models(folderpath):
        models = pd.DataFrame(glob.glob(f"{folderpath}/*.JSON"), columns = ['file'])
        if models.empty:
            return models
        models['model'] = models['file'].str.extract(model_regex)
        return models
    
    ###############
    # Prediction #
    ##############
    
    def collect_prediction_dataset(self, timestamp):
        """
        Return a pandas with the logs relevants for timestamp.
        For example, this could be 
        
        """
        return pd.DataFrame.from_dict( ['not', 'implemented', 'yet'] )
    
    def predict(self, T):
        print( "[%s] overwrite me to  predict. For now, everything is not true." % self.__class__.__name__)

        # Just one case is successfull
        self.last_prediction = T == "2020-01-18T00:00:00.000"
        # In theory, this should be something like:
        """
        dataset = self.dataset_collect_around(T)
        return self.model.predict( dataset )
        """
        return self.last_prediction
    
    ############
    # Training #
    ############
    
    def collect_training_dataset(self, start_ts, end_ts):
        """
        Return a pandas DataFrame with logs between start_ts and end_ts
        matching the query
        """
        return pd.DataFrame.from_dict( ['not', 'implemented', 'yet'] )
    
    def fit(self):
        """
        Traing model on dataset colected by collect_training_dataset method
        Saves the model inside the class.
        """
        model = None
        
        
    ####################################################
    # Information for human users, or users in general #
    ####################################################
    def human_last_predict(self):
        return "[%s] Prediction was %s. I don't have any other reason. " % ( self.__class__.__name__, self.last_prediction )
    
    def human_explain_failure(self):
        return "[%s] I don't have yet a human readable message to explain the last prediction result" % self.__class__.__name__
