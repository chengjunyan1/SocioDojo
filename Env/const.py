

ERRDOM='ERROR:DOMAIN' # invalid domain


# PROBE

PROBECODE={
    'WEB':'web',
    'IMG':'images',
    'NWS':'news',
    'YTB':'youtube',
    'FRG':'froogle',
    'FIN':'price',
    # 'VLM':'volume',
    'FRD':'fred',
    'FTE':'fte',
    'YGV':'yg'
}

ERRINV='ERROR:INVALID' # timeseries not usable, or no future data, cannot evaluate
ERRTMO='ERROR:TIMEOUT'
 
ERRUNF='ERROR:UNFOUND' # not found in listing
ERRYF='ERROR:YFINANCE'
ERRFRED='ERROR:FRED'


# QUERY

QUERYCODE={
    'SYS':'System', # system query, not OS
    'WKI':'Wiki',
    'GGL':'Google',
    'PRB':'Probe', # probe listings
    'NET':'Internet', # search concept, Wiki+Google
    # 'SEC':'SEC-Edgar', # search SEC filings
}

ERRREQ='ERROR:REQUEST' # Error from request
ERRGOOG='ERROR:GOOGLE' # Google search error
ERRNET='ERROR:NETSEARCH'


# HYPER PORTFOLIO

ERREXC='ERROR:EXCESS' # Not enough excess
ERRTRA='ERROR:TRADE' # Trade not allowed
ERRURL='ERROR:URL'
ERRREP='ERROR:REPEAT'


# WATCH

ERREMP='ERROR:EMPTY' # empty queue


# MODULE

ERRFAIL='ERROR:FAIL'


# WORLD

SYSQUIT='SYSTEM:QUIT'

COMMISSION=0.01 # commission rate
OVERNIGHT={
    'FRD':0.15,
    'WEB':0.15, # decaying 
    'YGV':-0.05, # year interest rate
    'FTE':-0.05, # year interest rate
    'FIN':0, # year divident rate
} # overnight year rate, for FRD, WEB; YGV, FTE not drastic change

BOUND=10 # upper and lower bound for gain and loss rate


Buggy=['FIN:AKAFF','FIN:PIGEF','FIN:KNBWF','FIN:KAOCF','FIN:AMDWF',
       'FTE:PSGV','FIN:NCTKF','FRD:H8B3094NSMCAG','FRD:WORAL',
       'FIN:EBCOF','FIN:PORBF','FIN:BDVC','FIN:RYBIF']


