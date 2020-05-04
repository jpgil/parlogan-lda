from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
pd.options.mode.chained_assignment = None

class Error(Exception):
   """Base class for other exceptions"""
   pass

class LargeElasticQuery(Error):
   """Raised when the query response is over 10000 hits"""
   pass

class EmptyElasticQuery(Error):
   """Raised when the query response is empty"""
   pass
