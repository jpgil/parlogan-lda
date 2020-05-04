import pandas as pd
import numpy as np
import networkx as nx
import json
import glob
import os
import numpy as np


class CommonLogs:
    """Class for the common log algorithm.

    Attributes:
        model: trained model
    """
    
    def __init__(self):
        self.model = None
    
    def is_trained(self):
        """Returns if there is a trained model. """
        return self.model != None
    
    def preprocess_dataset(self, dataset):
        """Delivers a preprocessed dataset. If the dataset has an incorrect format, it returns False.

        Args:
            dataset (DataFrame): dataframe with sequences of logs.

        Returns:
            (DataFrame, Bool): Preprocessed dataset or False.
        """
        if isinstance(dataset, pd.DataFrame):
            if list(dataset.columns) == ['@timestamp', 'symbol_seq']:
                if list(dataset.index.names) == ['seq_id', 'log_id']:
                    #then the format is correct
                    #get the index for every sequence
                    seq_ids = dataset.index.get_level_values('seq_id').unique()
                    #split dataframe in a list of sequences
                    log_sequences = [dataset.loc[seq_id] for seq_id in seq_ids]
                    #turn the sequences into an array of logs
                    return [np.array(df["symbol_seq"]) for df in log_sequences]
        return False
    
    def preprocess_logseq(self, log_seq):
        if isinstance(log_seq, pd.DataFrame):
            if list(log_seq.columns) == ['@timestamp', 'symbol_seq']:
                return list(log_seq['symbol_seq'])
        return False

    def fit(self, dataset, fixed_points, noise_tol = 1):
        #init the list of splitted logs
        log_seq_split = [[] for i in range(len(fixed_points)*2 - 1)]
        dataset = self.preprocess_dataset(dataset)
        
        if not dataset:
            print("Bad dataset format!")
            return
        
        #loop for each log sequence in the dataset
        for log_seq in dataset:
            #get the index of every fixed_point
            cuts = [np.where(log_seq == point)[0][0] for point in fixed_points[1:-1]]
            #adding start and end
            cuts = [0 ,] + cuts + [len(log_seq)-1, ]
            
            #cutting every log sequence into sets, according to the fixed points
            #sets corresponding at the same step in the secuence, go together in a list
            for i in range(len(cuts)-1):
                log_seq_split[2*i].append(np.array([fixed_points[i],]))
                log_seq_split[2*i+1].append(np.unique(np.array(log_seq[cuts[i]+1: cuts[i+1]])))
            log_seq_split[-1].append(np.array([fixed_points[-1],]))
        
        #To get the model, first we concatenate each set living in the same step
        log_seq_concat = [np.concatenate(step) for step in log_seq_split]
        #We count every log appereance
        log_seq_cnt = [np.unique(step, return_counts = True) for step in log_seq_concat]
        #We get the density of every log in each step, and store it in a dict
        log_seq_dict = [dict(zip(step[0], step[1]/len(dataset))) for step in log_seq_cnt]
        
        #Finally we apply the tolerance and delete every value with a density lower than the tolerance
        self.model = {
            'common_logs': [{key for key, val in step.items() if val >= noise_tol} for step in log_seq_dict],
            'fixed_points': fixed_points,
        }
    
    def predict(self, log_seq_df):
        #from dataframe of logs to list of logs
        if not self.is_trained():
            return
        
        log_seq = self.preprocess_logseq(log_seq_df)
        if not log_seq:
            print('Bad instance!')
            return
        
        #prediction results in a graph
        G = nx.Graph()
        node_id = 0
        x = 1
        sub_node_id = len(self.model['fixed_points'])
        
        log_seq = np.array(log_seq)
        fits_model = True
        missing_list = []
        fixed_points = self.model['fixed_points']
        common_logs = self.model['common_logs']
        
        #for each pair of fixed points, check every log in the model appears
        for i in range(len(fixed_points) - 1):
            points = fixed_points[i:i+2]
            cuts = [np.where(log_seq == p)[0] for p in points]
            
            #last iteration
            if i == len(fixed_points) - 2:
                #if last log missing, add it to the missing logs
                if cuts[1].size == 0:
                    cuts[1] = np.array([log_seq.shape[0], ])
                    missing_list.append({
                        "log": points[1],
                        "fixedPointBefore": points[0],
                        "fixedPointAfter": points[1],
                    })
                    G.add_node(node_id+1, pos = (x+1, 0), label = points[1], size = 50, color = 'red')
                #if not
                else:
                    G.add_node(node_id+1, pos = (x+1, 0), label = points[1], size = 50, color = 'skyblue')
                
            
            check_between = True
            
            #if a fixed point is missing, report it
            if cuts[0].size == 0:
                missing_list.append({
                    "log": points[0],
                    "fixedPointBefore": points[0],
                    "fixedPointAfter": points[1],
                })
                #if is missing a final log, we can check in between eather way
                check_between = False
                fits_model = False
                G.add_node(node_id, pos = (x, 0), label = points[0], size = 50, color = 'red')
            else:
                G.add_node(node_id, pos = (x, 0), label = points[0], size = 50, color = 'skyblue')
                    
            if cuts[1].size == 0:
                missing_list.append({
                    "log": points[1],
                    "fixedPointBefore": points[0],
                    "fixedPointAfter": points[1],
                })
                #if is missing a final log, we can check in between eather way
                check_between = False
                fits_model = False
                        
            
            #if no fixed point is missin, check the logs between them.
            if check_between or i == len(fixed_points) - 2:
                #assumption: not repeated fixed points
                between_logs = set(log_seq[cuts[0][0]+1: cuts[1][0]])
                must_logs = common_logs[2*i+1]
                #missing = common_logs[2*i+1].difference(between_logs)
                
                delta_x = .6/len(common_logs[2*i+1])
                x_node = x + .2
                for log in must_logs:
                    y_node = .5 * np.random.uniform(-1, 1)
                    if log not in between_logs:
                        missing_list.append({
                            "log": log,
                            "fixedPointBefore": points[0],
                            "fixedPointAfter": points[1],
                        })
                        fits_model = False
                        G.add_node(sub_node_id,pos=(x_node,y_node),label = log, size = 20, color = 'red')
                    else:
                        G.add_node(sub_node_id,pos=(x_node,y_node),label = log, size = 20, color = 'lightgreen')
                    x_node += delta_x
                    sub_node_id += 1
                    
            if node_id != 0:
                G.add_edge(node_id, node_id - 1)
            if i == len(fixed_points) - 2:
                G.add_edge(node_id+1, node_id)
            x += 1
            node_id += 1
                        
        missing_df = pd.DataFrame(missing_list)
        return fits_model, missing_df, G
    
    def get_model_graph(self):
        G = nx.Graph()
        #last_step = []
        node_id = 0
        x = 0
        sub_node_id = len(self.model['common_logs'])
        for step in self.model['common_logs']:
            
            if len(step) != 1:
                
                for node in step:
                    r = np.sqrt(np.random.uniform())
                    theta = np.random.uniform() * 2 * np.pi
                    
                    x_node = x-.5 + (.3 * r * np.cos(theta))
                    y_node = .5* r * np.sin(theta)
                    
                    G.add_node(sub_node_id, pos = (x_node , y_node), label = node, size = 20, color = '#C78CFB')
                    sub_node_id += 1
            
            else:
                fp_node = list(step)[0]
                G.add_node(node_id, label = fp_node, pos = (x, 0), size = 50, color = 'skyblue')
                if node_id != 0:
                    G.add_edge(node_id-1, node_id)
                node_id += 1
                x += 1
        return G
    
    def get_model(self):
        return self.model
    
    def save_model(self, filepath):
        if self.is_trained():
            with open(f"{filepath}.JSON", 'w') as out:
                #set is no json serializable
                model_to_save = {
                    'common_logs': [list(log_set) for log_set in self.model['common_logs']],
                    'fixed_points': self.model['fixed_points']
                }
                json.dump(model_to_save, out)
                return True
            return False
        return False
    
    def load_model(self, filepath):
        if os.path.isfile(f"{filepath}.JSON"):
            with open(f"{filepath}.JSON", 'r') as model:
                #set is no json serializable
                loaded_model = json.load(model)
                loaded_model['common_logs'] = [set(list_logs) for list_logs in loaded_model['common_logs']]
                self.model = loaded_model
                return True
            return False
        return False
