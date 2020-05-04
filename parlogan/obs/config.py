import pandas as pd
pd.options.mode.chained_assignment = None
import os
import sys
from parlogan.db.es import log_scan, EmptyElasticQuery
from parlogan.systems import related_system_errors
from parlogan import format_to_datetime, package_directory
import networkx as nx

params_regex = r"(?P<params>[A-Z0-9]+[A-Z0-9/. ]*) = (?P<value>.*)"
params_regex_wogroups = r"[A-Z0-9]+[A-Z0-9/. ]* = .*"
raw_data_folder = package_directory + '/cache/'
kibana_format = "http://kibana.datalab.pl.eso.org/app/kibana#/discover?_g=\
(time:(from:'%s',mode:absolute,to:'%s'))\
&_a=(columns:!(system,logtext),interval:auto,query:(language:lucene,query:'%s')\
,sort:!('@timestamp',desc))"
