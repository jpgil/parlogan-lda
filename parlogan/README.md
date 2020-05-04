# parlogan

Log analysis framework specialized in paranal activities. Mostly used in the LogsDashboard project https://gitlab.eso.org/rodriguj/logdashboards.

## Structure

The following scheme resumes the structure of the library

<img src="https://i.imgur.com/h58ZH59.png" alt="drawing" width="600"/>

## parlogan

parlogan has a series of functions that are not grouped in any subject (for now):

* `in_between(DataFrame dataset, string query_start, string query_end, string time_column = '@timestamp')`
* `format_to_datetime(timestamp timestamp)`

## color

color has the functions to *colorize* logs, i.e., remove words that don't add meaning to the log, remove variables, and so on. Methods:
* `colorize(string l)`
* `paranal(string l)`
* `SELINS(string l)`
* `PRESET(string l)`

## db

db integrates all the methods related to databases. To this day, there is only one database implemented, and that is Elasticsearch.

### es

All the methods for queries and scans are here, alongside the error classes when a query is too large or empty. List of methods
* `log_query(string query, timestamp start, timestamp end, string index = 'vltlog*')`
* `log_scan(string query, timestamp start, timestamp end, string index = 'vltlog*')`

## info

error_counting is the only function integrated here.

## model

Here are all the models for log analysis. All of the *paranalized* models extend from the `BaseModelParanal` class.

* `CommonLogs`
* `CommonLogsPARANAL(string query, list[string] fixed_points, float noise_tol = 1.)`: extends from `CommonLogs` and `BaseModelParanal`.
* `CommonLogsSELINS(string system, string selins_mode, float noise_tol = 1.)`: extends from `CommonLogsPARANAL`

## obs

All the functions related to analysis of OBs are grouped here.

* `OBs_between(string system, timestamp start, tiemstamp end)`
* `OB_describe(Series OB, string system, bool prev_next = True, bool bobby_mode = True)`

OB_describe uses the following auxiliary methods
* `OB_status(Series OB)`
* `OB_logs_between(timestamp start, timestamp end, string system)`
* `get_OB_name_id(timestamp start, timestamp end, string system)`
* `get_OB_proc_name_id(DataFrame OB_logs)`
* `get_OB_kibana_url(timestamp start, timestamp end, string system)`
* `get_OB_errors(DataFrame OB_logs)`
* `get_surrounding_OBs(timestamp OB_start, timestamp OB_end, string system)`
* `get_OB_params(DataFrame OB_logs)`
* `get_OB_blocks(DataFrame OB_logs)`
* `get_block_params(DataFrame block, int max_levels = 2)`

## systems

Systems has every function related to telescopes, instruments and all the systems in paranal.

* `telescope_night(timestamp timestamp, string telescope)`
* `instrument_night(timestamp timestamp, string instr)`
* `telescope_by_instrument(timestamp timestamp, string instr)`
* `VLTi_telescop_type(timestamp timestamp)`
* `instrument_related_systems(timestamp timestamp, string instr)`
* `related_system_errors(timestamp start, timestamp end, string instr)`










