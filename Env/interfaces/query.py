import os,requests,json
import requests
import wikitextparser as wtp 
import yfinance as yf
import pandas as pd
from fredapi import Fred
import secedgar 
from urllib.parse import urlencode
from urllib.request import urlopen

from Env.const import PROBECODE,QUERYCODE,ERRREQ,ERRGOOG,ERRDOM,ERRUNF,ERRYF,ERRTMO,ERRURL,ERRNET
from Env.utils import date2str,date_compare,str2date,replace_forbidden_chars,utc2str,str2utc
from Env.google_search import search


"""
Query external sources
"""

def get_txt_from_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        return ERRURL+f" Failed to download file. Status code: {response.status_code}"



class WikiSearch:
    def __init__(self,root,apikeys,num_results=5,cache=True):
        path=os.path.join(root,'Corpus','cache','Wiki')
        self.app_name=apikeys['wiki_app_name']
        self.api_token=apikeys['wiki_api_token']
        self.num_results=num_results
        self.cache=cache
        self.cache_dir=os.path.join(path,'cache')
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def get_history(self,page,current_time): # TODO: how to parse page
        url=f'https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles={page}&rvlimit=1&rvprop=content&rvdir=older&rvstart={current_time}&format=json'
        headers = {
            'Authorization': self.api_token,
            'User-Agent': self.app_name
        }
        response = requests.get(url, headers=headers)
        data = response.json()

        pages=data['query']['pages']
        for i in pages: break
        wikitext=pages[i]['revisions'][0]['*']
        return wikitext

    def search(self,search_query,filter=True):
        fname=replace_forbidden_chars(search_query)
        if os.path.exists(os.path.join(self.cache_dir,fname+'.json')):
            with open(os.path.join(self.cache_dir,fname+'.json'),'r') as f: pages=json.load(f)
            if pages=={}: return ERRUNF+' not found this term in Wikipedia'
            return pages
        language_code = 'en'
        headers = {
            # 'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
            'User-Agent': self.app_name
        }
        base_url = 'https://api.wikimedia.org/core/v1/wikipedia/'
        endpoint = '/search/page'
        url = base_url + language_code + endpoint
        parameters = {'q': search_query, 'limit': self.num_results}
        retry=0
        while True:
            try:
                response = requests.get(url, headers=headers, params=parameters)
                if not response.status_code // 100 == 2:
                    return ERRREQ+f" Request failed with status code: {response.status_code}"
                break
            except requests.RequestException as e:
                    retry+=1
                    if retry>=3:
                        return ERRREQ+f" An error occurred during the request, error message from compiler: {e}"
        data=response.json()['pages']
        if filter:
            pages={}
            for i in range(len(data)):
                p=data[i]
                page={}
                page['title']=p['title']
                page['excerpt']=p['excerpt']#.replace('<span class="searchmatch">','[').replace('</span>',']')
                page['description']=p['description']
                pages[f'top {i+1} result']=page
        if pages=={}: return ERRUNF+' not found this term in Wikipedia'
        if self.cache:
            with open(os.path.join(self.cache_dir,fname+'.json'),'w') as f: json.dump(pages,f)
        return pages

def test_ws(root,apikeys):
    print('|----------Test Wiki Query----------|')
    wiki=WikiSearch(root,apikeys)
    page = 'Apple'
    res=wiki.search(page)
    print(res)
    print('|----------Wiki Query Pass----------|')
    print()



class GoogleSearch:
    def __init__(self,root,num_result=5,cache=True):
        path=os.path.join(root,'Corpus','cache','Google')
        self.num_results=num_result
        self.cache=cache
        self.cache_dir=os.path.join(path,'cache')
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def search(self,query,time=None,parse=False):
        date=None
        if time is not None:
            date=utc2str(time).split(' ')[0]
        fname=replace_forbidden_chars(query)
        if date is not None:
            query+=' before:'+date
            dir=os.path.join(self.cache_dir,date)
            if os.path.exists(os.path.join(dir,fname+'.json')):
                with open(os.path.join(dir,fname+'.json'),'r') as f: return json.load(f)
        retry=0
        while True:
            try:
                r=search(query,advanced=True,num_results=self.num_results)
                res={}
                cnt=0
                for i in r:
                    cnt+=1
                    ri={}
                    ri['url']=i.url
                    ri['title']=i.title
                    ri['description']=i.description
                    res[f'top {cnt} result']=ri
                break
            except Exception as e:
                retry+=1
                if retry>=1:
                   return ERRGOOG+f" Google search failed, maybe timeout, error message from compiler: {e}"
        if date is not None and self.cache:
            if not os.path.exists(dir): os.makedirs(dir)
            with open(os.path.join(dir,fname+'.json'),'w') as f: json.dump(res,f)
        if parse: res=self.parse(query,date,res)
        return res
    
    def parse(self,query,date,res):
        if 'RESULT' in res: return res
        if len(res)==0: return ERRNET+' Found nothing in google for your query: '+query
        output_string = ""
        if date is not None:
            output_string += f"Query date: {date}\n\n"

        output_string += f'\nTop {len(res)} Google Results for the Query: "{query}"\n\n'
        ind=0
        for key, result in res.items():
            if ind>=self.num_results: break
            output_string += f"Result {key}:\n\n"
            output_string += f"URL: {result['url']}\n"
            output_string += f"Title: {result['title']}\n"
            output_string += f"Description: {result['description']}\n\n"
            ind+=1
        return output_string
    
    
def test_gs(root):
    print('|----------Test Google Query----------|')
    google=GoogleSearch(root)
    date='2009-01-01'
    t=pd.to_datetime(date, utc=True) # datestr or standard time
    res=google.search("what is Apple",t)
    print(res)
    print('|----------Test Google Query----------|')
    print()



class FREDSearch:
    def __init__(self,root,apikeys,cache=True):
        self.path=os.path.join(root,'Corpus','cache','FRED','search')
        self.fred = Fred(api_key=apikeys['fred_api_key'])
        self.cache=cache
        if cache and not os.path.exists(self.path):
            os.makedirs(self.path)

    def search(self,query):
        fname=replace_forbidden_chars(query)
        dir=os.path.join(self.path,fname+'.csv')
        try:
            if os.path.exists(dir): res=pd.read_csv(dir)
            else: 
                res=self.fred.search(query)
                if self.cache: res.to_csv(dir)
            if 'series id' in res.columns: res.set_index('series id',inplace=True)
            return res
        except Exception as e:
            print(e)
            return ERRTMO+f' during FRED search, could be network issue, error message from compiler: {e}'

    def search_note(self,query):
        res=self.search(query)
        if 'ERROR' in res: return res
        if query not in res.index: return ERRUNF+' during FRED search, queried code not found'
        res=res.loc[query]
        return res
    
    def search_code(self,query): # may return multiple codes, decide by agent
        return self.search(query)


def test_fs(root,apikey):
    print('|----------Test FRED Query----------|')
    fred=FREDSearch(root,apikey)
    res=fred.search_note("GDP")
    print(res)
    print('|----------Test FRED Query----------|')
    print()



class NetSearch:
    """
    Input: QUERY YYYY-MM-DD
    """
    def __init__(self,root,apikeys,num_results=5,cache=True,whatis=True):
        self.num_results=num_results
        self.whatis=whatis
        self.gs=GoogleSearch(root,num_results,cache)
        self.ws=WikiSearch(root,apikeys,num_results,cache)

    def search(self,query,time=None,parse=True):
        date=None
        if time is not None:
            date=utc2str(time).split(' ')[0]
        res={}
        metadata={}
        metadata['query']=query
        metadata['query date']=date if date is not None else 'NA'
        res['metadata']=metadata
        res[f'Top results of searching "{query}" in Wikipedia']=self.ws.search(query)
        gquery=f"What is {query}" if self.whatis else query
        res[f'Top results of searching "{gquery}" in Google']=self.gs.search(f"{gquery}",time)
        if parse: res=self.parse(res)
        return res
    
    def parse(self,data):
        query = data['metadata']['query']
        _,wiki,google=list(data.keys())
        wikipedia_results = data[wiki]
        google_results = data[google]

        output_string = ""
        output_string += "Metadata:\n"
        output_string += f"Query: {data['metadata']['query']}\n"
        output_string += f"Query date: {data['metadata']['query date']}\n\n"

        if 'ERROR' not in wikipedia_results and len(wikipedia_results)>0:
            output_string += f'Top {len(wikipedia_results)} Wikipedia Results for the Query: "{query}":\n\n'
            for key, result in wikipedia_results.items():
                output_string += f"Result {key}:\n\n"
                output_string += f"Title: {result['title']}\n"
                output_string += f"Excerpt: {result['excerpt']}\n"
                output_string += f"Description: {result['description']}\n\n"
        output_string=output_string.replace('<span class="searchmatch">','').replace('</span>','')

        gquery=f"What is {query}" if self.whatis else query
        if 'ERROR' not in google_results and len(google_results)>0:
            output_string += f'\nTop {len(google_results)} Google Results for the Query: "{gquery}"\n\n'
            ind=0
            for key, result in google_results.items():
                if ind>=self.num_results: break
                output_string += f"Result {key}:\n\n"
                # output_string += f"URL: {result['url']}\n"
                output_string += f"Title: {result['title']}\n"
                output_string += f"Description: {result['description']}\n\n"
                ind+=1
        if ('ERROR' in wikipedia_results or len(wikipedia_results)==0) and ('ERROR' in google_results or len(google_results)==0): 
            return ERRNET+' Found nothing in wiki or google'
        return output_string



class ProbeSearch:
    """
    Input: PROBECODE:QUERY YYYY-MM-DD
    Output: information about the listing
    """
    def __init__(self,root,apikeys,cache=True):
        self.path=os.path.join(root,'Corpus','cache')
        self.cache=cache
        self.ns=NetSearch(root,apikeys)
        self.fs=FREDSearch(root,apikeys)

    def query_listing(self,d,q):
        dir='538' if d=='FTE' else 'YouGov'
        dir=os.path.join(self.path,dir)
        with open(os.path.join(dir,'listings.json'),'r') as f:
            listings=json.load(f)
        if q not in listings: return ERRUNF+' during ProbeSearch: queried symbol not in listing'
        metadata={}
        metadata['query']=q
        metadata['domain code']=d
        res={}
        res['metadata']=metadata
        res['info']=listings[q]
        return res
    
    def filter_df(self,df,date):
        cols=[]
        for i in df.columns:
            if not isinstance(i,str): i=date2str(i)
            elif 'Unnamed' in i: 
                cols.append(i)
                continue
            if date_compare(date,i)>=0:
                cols.append(i)
        return df[cols]

    def query_yf(self,d,q,date):
        ticker=yf.Ticker(q)
        try:
            symbol=ticker.info['symbol']
        except Exception as e:
            return ERRYF+f' error message from compiler: {e}'
        cache_dir=os.path.join(self.path,'Probe','YF',symbol)
        metadata={}
        metadata['query']=q
        metadata['domain code']=d
        if os.path.exists(cache_dir): # exist only when cached
            with open(os.path.join(cache_dir,'info.json'),'r') as f: info=json.load(f)
            qis=pd.read_csv(os.path.join(cache_dir,'qis.csv'))
            qbs=pd.read_csv(os.path.join(cache_dir,'qbs.csv'))
            qcf=pd.read_csv(os.path.join(cache_dir,'qcf.csv'))
        else:
            try:
                ticker=yf.Ticker(q)
                info={}
                keys=['address1','city','state','country','industry',
                    'industryDisp','sector','longBusinessSummary']
                    #'fullTimeEmployees','volume']
                for i in keys: 
                    try:
                        info[i]=ticker.info[i]
                    except: continue
                qis=ticker.quarterly_income_stmt
                qbs=ticker.quarterly_balance_sheet
                qcf=ticker.quarterly_cashflow
            except Exception as e:
                return ERRYF+f' during ProbeSearch: failed to download metadata from finance, error message from compiler: {e}'
        res={}
        res['metadata']=metadata
        res['info']=info
        qis_filter=self.filter_df(qis,date)
        qbs_filter=self.filter_df(qbs,date)
        qcf_filter=self.filter_df(qcf,date)
        if len(qis_filter.columns)>0: res['quarterly income statements']=qis_filter.to_csv(index=False)
        if len(qbs_filter.columns)>0: res['quarterly balance sheet']=qbs_filter.to_csv(index=False)
        if len(qcf_filter.columns)>0: res['quarterly cashflow']=qcf_filter.to_csv(index=False)
        if self.cache and not os.path.exists(cache_dir): 
            os.makedirs(cache_dir)
            with open(os.path.join(cache_dir,'info.json'),'w') as f: json.dump(info,f)
            qis.to_csv(os.path.join(cache_dir,'qis.csv'))
            qbs.to_csv(os.path.join(cache_dir,'qbs.csv'))
            qcf.to_csv(os.path.join(cache_dir,'qcf.csv'))
        return res

    def query_gt(self,d,q,date):
        res={}
        metadata={}
        metadata['query']=q
        metadata['domain code']=d
        res['metadata']=metadata
        res['Internet search results']=self.ns.search(q,date)
        return res

    def query_fred(self,d,q):
        res={}
        metadata={}
        metadata['query']=q
        metadata['domain code']=d
        res['metadata']=metadata
        note=self.fs.search_note(q)
        if not isinstance(note,str):
            res[f'FRED database note for "{q}"']=note.to_csv(index=False)
        return res
    
    def search(self,q,t):
        d=utc2str(t).split(' ')[0]
        head,query=q.split(':', 1)
        if not head in PROBECODE: return ERRDOM+' during ProbeSearch: no such domain for probe, probe domain codes: '+str(PROBECODE)
        if head in ['YGV','FTE']:
            res=self.query_listing(head,query)
        elif head in ['FIN','VLM']:
            res=self.query_yf(head,query,d)
        elif head=='FRD':
            res=self.query_fred(head,query)
        else:
            res=self.query_gt(head,query,d)
        return res

def test_ps(root,apikeys):
    print('|----------Test Probe Query----------|')
    ps=ProbeSearch(root,apikeys)
    q='FRD:GDP'# YGV:FOEC.NS.B FTE:MRPU.B FIN:AAPL FRD:GDP
    d="2023-01-01"
    t=pd.to_datetime(d, utc=True) # datestr or standard time
    res=ps.search(q,t)
    print(res)
    print('|----------Probe Query Pass----------|')
    print()


class SECSearch:
    def __init__(self,root,apikeys,cache=True):
        self.cache=cache
        self.path=os.path.join(root,'Corpus','cache','SEC','filings')
        self.user_agent=apikeys['name_email'] # "Name (email)"
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def download(self,ticker,datestr,type):
        filing = secedgar.filings(cik_lookup=ticker,
                        filing_type=type,
                        # start_date=date(2015, 1, 1),
                        count=1,
                        end_date=str2date(datestr),
                        user_agent=self.user_agent)
        url=filing.get_urls()[ticker][0]
        dir=os.path.join(self.path,ticker,type.value,url.split('/')[-1])
        print(dir)
        if self.cache and not os.path.exists(dir):
            filing.save(self.path)
        if os.path.exists(dir):
            with open(dir,'r') as f: 
                content=f.read()
        else: 
            content=get_txt_from_url(url)
        return content

    def search(self,ticker,t,ftype=None): # default get latest 10k 10q of the date
        d=utc2str(t).split(' ')[0]
        if ftype is None:
            tenk=self.download(ticker,d,secedgar.FilingType.FILING_10K)        
            tenq=self.download(ticker,d,secedgar.FilingType.FILING_10Q)
            return tenk,tenq 
        else:
            return self.download(ticker,d,ftype)

def test_sec(root,apikeys):
    print('|----------Test SEC Query----------|')
    sec=SECSearch(root,apikeys)
    d='2022-01-01'
    t=pd.to_datetime(d, utc=True) # datestr or standard time
    tenk,tenq=sec.search('AAPL',t)
    print('length 10k, 10q',len(tenk),len(tenq))
    print('|----------SEC Query Pass----------|')
    print()


class GKGSearch:
    """
    Google KG Search
    Not very intelligent, need exact search
    """
    def __init__(self,root,apikeys,num_results=5,cache=True):
        path=os.path.join(root,'Corpus','cache','GKG')
        self.api_key=apikeys['google_search']
        self.num_results=num_results
        self.cache=cache
        self.cache_dir=os.path.join(path,'cache')
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def search(self,q):
        fname=replace_forbidden_chars(q)
        if os.path.exists(os.path.join(self.cache_dir,fname+'.json')):
            with open(os.path.join(self.cache_dir,fname+'.json'),'r') as f: pages=json.load(f)
            if pages=={}: return ERRUNF+' not found this term in Wikipedia'
            return pages
        service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
        params = {
            'query': q,
            'limit': self.num_results,
            'indent': True,
            'key': self.api_key,
        }
        url = service_url + '?' + urlencode(params)
        retry=0
        while True:
            try:
                response = json.loads(urlopen(url).read())['itemListElement']
                break
            except Exception as e:
                retry+=1
                if retry>3: 
                    return ERRGOOG+f" Google search failed, maybe timeout, error message from compiler: {e}"
        res={}
        for i in response:
            res[i['result']['name'] + ' (' + str(i['resultScore']) + ')']=i
        if self.cache:
            with open(os.path.join(self.cache_dir,fname+'.json'),'w') as f: json.dump(res,f)
        return res


def test_gkg(root,apikeys):
    print('|----------Test Google KG Search----------|')
    gkg=GKGSearch(root,apikeys)
    q='what is google'
    res=gkg.search(q)
    print(res)

    print('|----------Google KG Search Pass----------|')
    print()

class UniQuery:
    """
    Input: HEAD:QUERY UTC time
    Output: json, or ERROR
    Query external sources or tools defined in QUERYCODE
    """
    def __init__(self,root,apikeys,num_results=3,cache=True):
        self.ws=WikiSearch(root,apikeys,num_results,cache)
        self.gs=GoogleSearch(root,num_results,cache)
        self.ns=NetSearch(root,apikeys,num_results,cache)
        self.fs=FREDSearch(root,apikeys)
        self.ps=ProbeSearch(root,apikeys)
        self.sec=SECSearch(root,apikeys)
        self.gkg=GKGSearch(root,apikeys)

    def query(self,q,t,data=None): # all search interface should be standard time
        t=pd.to_datetime(t, utc=True) # datestr or standard time
        head,query=q.split(':', 1)
        if not head in QUERYCODE: return ERRDOM+' no such domain for query/search, tool codes: '+str(QUERYCODE)
        if head=='WKI': res=self.ws.search(query)
        elif head=='GGL': res=self.gs.search(query,t)
        elif head=='GKG': res=self.gkg.search(query)
        elif head=='NET': res=self.ns.search(query,t)
        elif head=='PRB': # query: PROBECODE:QUERY, mainly for broker use
            res=self.ps.search(query,t)
        elif head=='SEC': res=self.sec.search(query,t) # query is a ticker, do not use right now, hard to parse
        return res

def test_uq(root,apikeys):
    print('|----------Test Uni Query----------|')
    uq=UniQuery(root,apikeys)
    d="2023-01-01"
    q='NET:What is the ticker for apple company'# YGV:FOEC.NS.B FTE:MRPU.B FIN:AAPL FRD:GDP
    res=uq.query(q,d)
    print(res)
    print(len(str(res)))
    q='YGV:FOEC.NS.B'# YGV:FOEC.NS.B FTE:MRPU.B FIN:AAPL FRD:GDP
    res=uq.query(q,d)
    q='FTE:MRPU.B'# YGV:FOEC.NS.B FTE:MRPU.B FIN:AAPL FRD:GDP
    res=uq.query(q,d)
    q='FIN:AAPL'# YGV:FOEC.NS.B FTE:MRPU.B FIN:AAPL FRD:GDP
    res=uq.query(q,d)
    q='FRD:GDP'# YGV:FOEC.NS.B FTE:MRPU.B FIN:AAPL FRD:GDP
    res=uq.query(q,d)

    print('|----------Uni Query Pass----------|')
    print()



