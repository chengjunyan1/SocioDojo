import copy


root="./"
sysddir='~/Downloads' # browser download dir

time_span="2021-10-01 2023-08-01" # 22 months window
init_time,end_time=time_span.split(' ')

apikeys={}
apikeys['wiki_app_name']='GET_IT_IN_WIKIDATA_API'
apikeys['wiki_api_token']='GET_IT_IN_WIKIDATA_API'
apikeys['fred_api_key']="GET_IT_IN_FRED_API"
apikeys['name_email']='YOUR_NAME_EMAIL'
apikeys['openai_apikey']='GET_IT_IN_OPENAI_API'
apikeys['google_search']='GET_IT_IN_GOOGLE_SEARCH_API'
apikeys['serp_apikey']='GET_IT_IN_SERP_API'

num_results=3

base_worldconfig={
    'init_time':init_time,
    'sysddir':sysddir,
    'num_results':num_results,
    'default_channels':['NYT','WSJ'],
}

sawi_worldconfig=copy.deepcopy(base_worldconfig)
sawi_worldconfig['type']='sawi'
sawi_worldconfig['period']=0

base_agentconfig={}


base_config={
    'apikeys':apikeys,
    'root':root,
    'end_time':end_time,
    'ruleset': ['nodt'],
}






