from .config import *

def colorize(l):
    """Applies substitutions in a string to remove irrelevant words or characters.

    Args:
        l (string): string to be modified

    Returns:
        (string): modified string
    """
    l = regex_UTCdate.sub( r"_date_", l )
    l = regex_symbols.sub( r" ", l )
    l = regex_stopwords.sub( r"\1\3", l )
    l = regex_numbers.sub( r"\1{}", l)
    l = re.sub(' {2,}', ' ', l) 
    return l.strip()

def paranal(l):
    """Applies substitutions in a string to remove irrelevant words or characters,
    according to paranal standards.

    Args:
        l (string): string to be modified

    Returns:
        (string): modified string
    """
    #replace in string l, any obs_param with 
    l = regex_obs_param.sub(r"\1 000", l)
    l = regex_procname.sub(r"\1_X", l)
    l = regex_fits.sub(r"0_fits", l)
    l = regex_telescope_prefix.sub(r"\1X\2", l)
    l = regex_cmd.sub(r"cmdX", l)
    return colorize(l)

def SELINS(l, mode = 'standard'):
    """Applies substitutions in a string to remove irrelevant words or characters,
    according to SELINS preferences.

    Args:
        l (string): string to be modified
        mode (string): SELINS mode. It can be standard, NA or CO.

    Returns:
        (string): modified string
    """
    l = regex_selins_start.sub(r"SELINS Buffer", l)
    l = regex_standby.sub(r"{X} focus STANDBY", l)
    l = regex_online.sub(r"{Y} focus ONLINE", l)
    if mode == 'NA':
        l = regex_move_NA.sub(r"M2 M3 from NA {X}", l)
    elif mode == 'CO':
        l = regex_move_CO.sub(r"M2 M3 from CO {X}", l)
    else:
        l = regex_move_XY.sub(r"M2 M3 from {X} {Y}", l)
        l = regex_move_XX.sub(r"M2 M3 from {X} {X}", l)
    return paranal(l)

def PRESET(l):
    """Applies substitutions in a string to remove irrelevant words or characters,
    according to PRESET preferences.

    Args:
        l (string): string to be modified

    Returns:
        (string): modified string
    """
    l = PRESET_start_re.sub(r'PRESET SETUP', l)
    l = PRESET_end_re.sub(r'PRESET SUCCESS', l)
    return paranal(l)
