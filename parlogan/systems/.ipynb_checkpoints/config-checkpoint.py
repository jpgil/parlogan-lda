import pandas as pd
pd.options.mode.chained_assignment = None
from parlogan.db.es import log_scan, EmptyElasticQuery
from parlogan.info import error_counting
import numpy as np
from datetime import datetime, time

VLTi_instruments = ["GRAVITY", "PIONIER", "MATISSE"]
