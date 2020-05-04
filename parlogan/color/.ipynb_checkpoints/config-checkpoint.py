import re

#color regex
regex_symbols = re.compile(r"\W+")
regex_numbers = re.compile(r"('|:|\,|^| |\t|\.|\()\-*\d+(\-\d\d\-\d\dT\d\d)?")
regex_stopwords = re.compile(r"(\W)(at|to|for|s|with|by|is|the|of)(\W)")
regex_UTCdate = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{0,3})?")

#paranal regex
regex_telescope_prefix = re.compile(r"([lw][au]{0,1}t)[0-9]([a-z]+)")
regex_cmd = re.compile(r"cmd[0-9]+")
regex_fits = re.compile(r"([A-Za-z\d_])+\.fits")
regex_procname = re.compile(r"([a-z]+)_\d{4,}")
regex_obs_param = re.compile(r"((OBS|SEQ|INS)\.[A-Z:\._]+ )[A-Za-z\d_]+")

#selins regex
regex_selins_start = re.compile(r"SELINS, Buffer.*")
regex_move_XX = re.compile(r"M2 & M3 from ([A-Z]{2}) to \1")
regex_move_XY = re.compile(r"M2 & M3 from ([A-Z]{2}) to (?!\1)([A-Z]{2})")
regex_move_NA = re.compile(r"M2 & M3 from NA to ([A-Z]{2})")
regex_move_CO = re.compile(r"M2 & M3 from CO to ([A-Z]{2})")
regex_standby = re.compile(r"([A-Z]{2}) focus to STANDBY")
regex_online = re.compile(r"([A-Z]{2}) focus to ONLINE")

#preset regex
PRESET_start_re = re.compile(r"PRESET: Received command: SETUP.*")
PRESET_end_re = re.compile(r"PRESET: Succesfully completed.*")