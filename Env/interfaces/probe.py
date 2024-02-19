import os,time,json,random,math,re
import numpy as np
import pandas as pd
import datetime
import functools as ft
from scipy.interpolate import interp1d
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
from pathos.multiprocessing import ProcessPool

import yfinance as yf
from pytrends.request import TrendReq
import pandas_datareader.data as web
from fredapi import Fred

from Env.const import Buggy


"""
1. All query(q) returns date (index) - value dataframe
2. Each class should have a listing table apart from gtrends
Notes:
* Interpolate inside each class are experimental
* anchor in GTrend is experimental
"""

from Env.const import PROBECODE,ERRINV,ERRTMO,ERRDOM,ERRUNF,ERRYF,ERRFRED
from Env.utils import date_add,date2str,date_compare,str2date,translate_forbidden_chars,load_json,save_json,makedirs,get_icodes,replace_forbidden_chars
from Scraper.base_scraper import BaseScraper

pjoin=os.path.join
pexist=os.path.exists




_useragent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.62',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0',
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15"
]

def get_useragent():
    return random.choice(_useragent_list)


class GTDownloader(BaseScraper): # selenium based, slower but stabler
    def __init__(self,sysddir): # where is the default download dir of the browser
        super().__init__(headless=True)
        # self.user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.200"
        self.user_agent=get_useragent()
        self.fdir=os.path.join(sysddir,'multiTimeline.csv') 
        self.driver = self.create_driver()


    def query_downloader(self,q,gprop='web'):
        porp='' if gprop=='web' else gprop
        url=f'https://trends.google.com/trends/explore?date=all&geo=US&gprop={porp}&q={q}'
        self.driver.get(url)
        retry=0
        while True:
            try:
                self.wait(1)
                self.driver.refresh()
                WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH,'//button[@class="widget-actions-item export"]'))
                ).click()
                df=pd.read_csv(self.fdir)[1:]
                break
            except Exception as e: 
                retry+=1
                if retry>10: return ERRTMO+f' Google Trend timeout, error message from compiler: {e}'
        df.rename(columns={'Category: All categories': q}, inplace=True)
        os.remove(self.fdir)
        if len(df)==0: return ERRINV+' empty time series, maybe error from google or invalid keyword'
        return df


def test_gtd():
    gtd=GTDownloader('~/Downloads')
    df=gtd.query_downloader('apple')
    print(df)


class GT: # Society
    def __init__(self,root,cache=True,renorm=True,interpolate=True,sysddir=None): # change time need change chains
        # Configs, change them need to reconstruct the chains, especially time
        self.interpolate_mode="rand"
        self.interpolate=interpolate
        self.gtd=None
        if sysddir is not None:
            self.gtd=GTDownloader(sysddir)

        self.path=os.path.join(root,'Corpus','cache','GTrends')
        if not os.path.exists(self.path): os.makedirs(self.path)
        self.update_logs={}
        for gprop in ['web','images','news','youtube','froogle']:
            path=os.path.join(self.path,gprop)
            if not os.path.exists(path): os.makedirs(path)
            self.update_logs[gprop]={}
            log_dir=os.path.join(path,'update_log.json')
            if os.path.exists(log_dir):
                with open(log_dir,'r') as f: self.update_logs[gprop]=json.load(f)

        self.requests_args = {
            # 'headers': {
            # "Host": "trends.google.com",
            # "User-Agent": get_useragent(),
            # "Accept": "application/json, text/plain, */*",
            # "Accept-Language": "en-US,en;q=0.5",
            # "Accept-Encoding": "gzip, deflate, br",
            # "Alt-Used": "trends.google.com",
            # "Connection": "keep-alive",
            # "Referer": "https://trends.google.com/trends/explore?date=now%201-d&geo=US&q=apple&hl=en-US",
            # "Cookie": '',
            # "Sec-Fetch-Dest": "empty",
            # "Sec-Fetch-Mode": "cors",
            # "Sec-Fetch-Site": "same-origin",
            # "TE": "trailers"
            # }
        }
        self.cache=cache

    def gtrend_call(self,kw_list,gprop='web'):#,save=False):
        tend=date2str(datetime.datetime.now())
        tstart='2004-01-01' #date_add(tend,y=-5) #'2004-01-01'
        timeframe=tstart+' '+tend
        pytrends = TrendReq(hl='en-US', geo='US', tz=360,requests_args=self.requests_args)
        porp='' if gprop=='web' else gprop
        pytrends.build_payload(kw_list, cat=0, timeframe=timeframe, geo='US', gprop=porp)
        df=pytrends.interest_over_time()
        if len(df)==0: return ERRINV+' empty time series, maybe error from google or invalid keyword'
        return df

    def renormalizer(self):
        if np.random.rand()<=0.5:
            return random.uniform(0.01,1)
        else:
            return random.uniform(1,10)


    def query_renorm(self,q,t,gprop='web',renormalizer=None): # faster, problem is that it is still leakable, since there is a max
        dir=os.path.join(self.path,gprop,'cache_renorm')
        if not os.path.exists(dir): os.makedirs(dir)
        log_dir=os.path.join(self.path,gprop,'update_log.json')

        savename=replace_forbidden_chars(q)
        update_log=self.update_logs[gprop]
        if savename in update_log:
            last_update=update_log[savename]
            if date_compare(date2str(t),last_update)<=0:
                ret=pd.read_csv(os.path.join(dir,savename+'.csv'))
                for i in ret.columns:
                    if i!='value': ret = ret.rename(columns={i: 'date'})
                ret.set_index('date', inplace=True)
                ret.index = pd.to_datetime(ret.index)
                ret=ret.sort_index()
                return ret
        try:
            df=self.gtrend_call([q],gprop)
        except Exception as e:
            if self.gtd: # retry with selenium
                df=self.gtd.query_downloader(q,gprop) 
            else: return ERRTMO+f' Google Trend timeout, error message from compiler: {e}'
        if 'ERROR' in df: return df
        ret=df[[q]]
        # if self.interpolate: ret=df[['date',q]]
        ret=ret.rename(columns={q:'value'})
        values=df[q].values.astype(float)
        ret['value']=values*self.renormalizer() if renormalizer is None else values*renormalizer
        # if self.interpolate: ret.set_index('date',inplace=True)
        ret.index = pd.to_datetime(ret.index)
        ret=ret.sort_index()
        if self.cache: 
            ret.to_csv(os.path.join(dir,savename+'.csv'))
            update_log[savename]=str(datetime.date.today())
            with open(log_dir,'w') as f:
                json.dump(update_log,f)
        return ret
    
    def check_df(self,df,end='2021-11-01',minlen=28):
        if len(df)<3: return False
        first_updated=date2str(df.index[0])
        last_updated=date2str(df.index[-1])
        if date_compare(last_updated,first_updated)<minlen: return False
        if date_compare(last_updated,end)<0: return False
        # if date_compare(first_updated,start)>0: return False
        # print(first_updated,last_updated,len(df))
        return True
    
    def query(self,q,t,domain='web',renormalizer=None):
        t=pd.to_datetime(t, utc=True)
        query_fn=self.query_renorm #if self.renorm else self.query_anchor
        df=query_fn(q,t,domain,renormalizer)
        if 'ERROR' in df: return df
        df = df[df['value'] != 0]
        if not self.check_df(df): return ERRINV+' invalid time series, too short'
        if self.interpolate:
            values=df['value'].values
            bottom=values.min()*0.8 # avoid extreme values
            top=values.min()*5
            values[values <= 0] = bottom
            values[values>top]=top
            interp_func = interp1d(df.index.astype(np.int64), values, kind='cubic')
            ndate = pd.date_range(start=date2str(df.index[0]), end=date2str(df.index[-1]), freq='D')
            nvalue = interp_func(ndate.astype(np.int64))
            nvalue[nvalue < bottom] = bottom
            df = pd.DataFrame({'date': ndate, 'value': nvalue})
            df.set_index('date',inplace=True)
            df.index = pd.to_datetime(df.index)
            df=df.sort_index()
        return df

def test_gt(root):
    print('|----------Test GT Probe----------|')
    t='2023-08-13'
    sysddir='~/Downloads'
    sysddir=None
    gt=GT(root,cache=True,sysddir=sysddir)
    df=gt.query('boa',t)
    print(df)
    print('|----------GT Probe Pass----------|')
    print()

    


class YF: # Market
    """
    Close price is not good, aftermarket cannot be reflected
    """
    def __init__(self,root,cache=True):
        self.path=os.path.join(root,'Corpus','cache','YF')
        self.cache=cache
        if not os.path.exists(os.path.join(self.path,'cache')): 
            os.makedirs(os.path.join(self.path,'cache'))
        self.update_log={}
        self.log_dir=os.path.join(self.path,'update_log.json')
        if os.path.exists(self.log_dir):
            with open(self.log_dir,'r') as f: self.update_log=json.load(f)

    def run_query(self,q,t):
        ticker=yf.Ticker(q)
        try:
            symbol=ticker.info['symbol']
        except Exception as e:
            return ERRYF+f' error message from compiler: {e}'
        dir=os.path.join(self.path,'cache',symbol+'.csv')
        if symbol in self.update_log:
            last_update=self.update_log[symbol]
            if date_compare(date2str(t),last_update)<=0:
                data=pd.read_csv(dir)
                data.set_index('Date', inplace=True)
                data.index = pd.to_datetime(data.index)
                data=data.sort_index()
                return data
        hist=ticker.history(start='1970-01-01') # all history data
        if len(hist)<30: return ERRYF+' data not found'
        data=hist[['Close','Volume']]
        data=data.rename(columns={'Close':'price','Volume':'volume'})
        data.index = pd.to_datetime(data.index)
        data=data.sort_index()
        if self.cache: 
            data.to_csv(dir)
            self.update_log[symbol]=str(datetime.date.today())
            with open(self.log_dir,'w') as f:
                json.dump(self.update_log,f)
        return data

    def query(self,q,t,domain='price'): # domain: price or volume
        t=pd.to_datetime(t, utc=True)
        data=self.run_query(q.upper(),t)
        if 'ERROR' in data: return data
        data=data[[domain]]
        data=data.rename(columns={domain:'value'})
        return data

def test_yf(root):
    print('|----------Test YF Probe----------|')
    t="2023-07-01"
    t=datetime.datetime.now()
    yf=YF(root,cache=True)
    q='ANDHF'
    ret=yf.query(q,t,'volume')
    # print(ret)
    print('|----------YF Probe Pass----------|')
    print()



class FRED: # Economy
    def __init__(self,root,apikeys,cache=True):
        self.fred = Fred(api_key=apikeys['fred_api_key'])
        self.path=os.path.join(root,'Corpus','cache','FRED')
        self.cache=cache
        self.update_log={}
        self.log_dir=os.path.join(self.path,'update_log.json')
        if os.path.exists(self.log_dir):
            with open(self.log_dir,'r') as f: self.update_log=json.load(f)

    def search(self,q):
        ret=self.fred.search(q)[['id','notes']]
        return ret.iloc[0]['id'] # return the top match

    def load_csv(self,dir,fname):
        data=pd.read_csv(os.path.join(dir,fname+'.csv'))
        data.set_index('DATE', inplace=True)
        data.index = pd.to_datetime(data.index)
        data=data.sort_index()
        return data
    
    def query(self,q,t):
        t=pd.to_datetime(t, utc=True)
        q=q.upper()
        dir=os.path.join(self.path,'cache')
        if not os.path.exists(dir): os.makedirs(dir)
        if q in self.update_log:
            last_update=self.update_log[q]
            if date_compare(date2str(t),last_update)<=0:
                return self.load_csv(dir,q)
        else:
            try: # search code then load
                fname=self.search(q)
                last_update=self.update_log[fname]
                if date_compare(date2str(t),last_update)<=0:
                    return self.load_csv(dir,fname)
            except:
                pass
        try:
            data=web.DataReader(q, 'fred', '1970-01-01')
        except:
            try:
                q=self.search(q) # search load then download
                data=web.DataReader(q, 'fred', '1970-01-01')
            except Exception as e:
                return ERRFRED+f' query not found, error message from compiler: {e}'
        if len(data)<5: return ERRINV+' empty time series'
        # if self.interpolate: data=df_interpolate(data,[q])
        data=data.rename(columns={q:'value'})
        # if self.interpolate: data.set_index('date', inplace=True)
        data.index = pd.to_datetime(data.index)
        data=data.sort_index()
        if self.cache: 
            data.to_csv(os.path.join(dir,q+'.csv'))
            self.update_log[q]=str(datetime.date.today())
            with open(self.log_dir,'w') as f:
                json.dump(self.update_log,f)
        return data

def test_fred(root,apikeys):
    print('|----------Test FRED Probe----------|')
    fred=FRED(root,apikeys,cache=True)
    q='GDP'
    t='2023-01-01'
    ret=fred.query(q,t)
    print(ret)
    print('|----------FRED Probe Pass----------|')
    print()



def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z_+]', '', input_string)

def get_code(raw,maxlen=7,seg=False,segmax=4):
    if not seg: raw=raw.replace('+','_')
    raw=remove_non_letters(raw)
    code=[]
    n=0
    for seg in raw.split('+'):
        if seg=='': continue
        count=0
        for j in seg.split('_'):
            if j!='': 
                count+=1
                code.append(j[0].upper())
                if count>=segmax: break
        code.append('.')
        n+=1
    code=''.join(code[:-1][:maxlen+n-1])
    return code

def int_to_letters(number):
    if number <= 0:
        raise ValueError("Invalid input. Please provide a positive integer.")
    result = ""
    while number > 0:
        number -= 1
        div, mod = divmod(number, 26)
        result = chr(mod + ord('A')) + result
        number = div
    return result

class FTE: # Politics
    def __init__(self,root,interpolate=True):
        self.path=os.path.join(root,'Corpus', 'cache','538')
        self.interpolate=interpolate

        if not os.path.exists(os.path.join(self.path,'metadata.json')):
            self.construct_time_series()
        self.listings=self.get_listings()

    def get_pte_dataframe(self,poll,cols=None,hist=True,low_memory=True):
        fname=poll+'.csv'
        df1=pd.read_csv(os.path.join(self.path,'polls',fname),low_memory=low_memory)
        if hist:
            fname=poll+'_historical.csv'
            df2=pd.read_csv(os.path.join(self.path,'polls',fname),low_memory=low_memory)
            df=pd.concat([df1[cols],df2[cols]]) if cols else pd.concat([df1,df2])
        else: df=df1[cols] if cols else df1
        return df
    
    def process_candidate_poll(self,df,ts,title,metadata):
        for _, row in df.iterrows():
            row=row.fillna('NA')
            candidate=row['candidate_name']
            office_type=row['office_type']
            state=row['state']
            party=row['party']
            date=row['end_date']
            value=row['pct']
            if title=='president_primary_polls':
                candidate+=' (Primary)'
            key=candidate+'+'+office_type+'+'+state+'+'+party
            key=key.replace(' ','_').replace('"','')
            if key not in ts:
                data={'date':[date],'value':[value]}
                ts[key]=pd.DataFrame(data)
                metadata[key]=self.get_metadata(row,title)
            else: 
                row=pd.DataFrame([{'date':date,'value':value}])
                ts[key]=pd.concat([ts[key], row], ignore_index=True)
        return ts,metadata

    def process_favorability_poll(self,df,ts,title,metadata):
        for _, row in df.iterrows():
            data={}
            row=row.fillna('NA')
            politician=row['politician']
            date=row['end_date']
            pos=row['yes'] if title!='favorability_polls' else 'favorable'
            neg=row['no'] if title!='favorability_polls' else 'unfavorable'
            for favo in ['favorable','unfavorable']:
                key=politician+f' ({favo})'
                key=key.replace(' ','_').replace('"','')
                value=pos if favo=='favorable' else neg
                if key not in ts:
                    data={'date':[date],'value':[value]}
                    ts[key]=pd.DataFrame(data)
                    metadata[key]=self.get_metadata(row,title+f' ({favo})')
                else: 
                    row=pd.DataFrame([{'date':date,'value':value}])
                    ts[key]=pd.concat([ts[key], row], ignore_index=True)
        return ts,metadata

    def get_metadata(self,df,title):
        keys=['state','office_type','seat_number','seat_name',
              'stage','partisan','election_date','party']
        md={'poll':title}
        for i in keys:
            if i not in df.index: md[i]='NA'
            else: md[i]=df[i]
        return md

    def construct_time_series(self,low_memory=False):
        path=os.path.join(self.path,'data')
        if not os.path.exists(path): os.makedirs(path)
        
        # polls={
        #     'generic_ballot_polls':['end_date','office_type','dem','rep'],
        #     'governor_polls':['end_date','office_type','state','party','candidate_name','pct'],
        #     'house_polls': ['end_date','office_type','state','party','candidate_name','pct'],
        #     'president_polls': ['end_date','office_type','state','party','candidate_name','pct'],
        #     'president_primary_polls': ['end_date','office_type','state','party','candidate_name','pct'],
        #     'senate_polls': ['end_date','office_type','state','party','candidate_name','pct'],
        #     'president_approval_polls': ['end_date','politician','yes','no'],
        #     'favorability_polls': ['end_date','politician','favorable','unfavorable'],
        #     'vp_approval_polls': ['end_date','politician','yes','no'],
        # }   
        ts={}
        metadata={}

        poll='generic_ballot_polls'
        df=self.get_pte_dataframe(poll,low_memory=low_memory)
        ts['Democratic_Party']=df[['end_date','dem']]
        ts['Republican_Party']=df[['end_date','rep']]
        ts['Democratic_Party']=ts['Democratic_Party'].rename(columns={'end_date':'date','dem':'value'})
        ts['Republican_Party']=ts['Republican_Party'].rename(columns={'end_date':'date','rep':'value'})
        metadata['Republican_Party']=self.get_metadata(df.iloc[0],poll)
        metadata['Democratic_Party']=self.get_metadata(df.iloc[0],poll)

        for i in ['governor_polls','house_polls','president_polls',
                  'president_primary_polls','senate_polls']:
            df=self.get_pte_dataframe(i,low_memory=low_memory)
            ts,metadata=self.process_candidate_poll(df,ts,i,metadata)
        
        for i in ['president_approval_polls','favorability_polls','vp_approval_polls']:
            hist=True if i=='president_approval_polls' else False
            df=self.get_pte_dataframe(i,hist=hist,low_memory=low_memory)
            ts,metadata=self.process_favorability_poll(df,ts,i,metadata)

        for i in ts: 
            ts[i].set_index('date', inplace=True)
            ts[i].to_csv(os.path.join(path,i+'.csv'))
        with open(os.path.join(self.path,'metadata.json'),'w') as f:
            json.dump(metadata,f)

    def check_listing(self,md,end='2021-11-01',minlen=28):
        df=self.read(md)
        if len(df)<3: return False
        first_updated=date2str(df.index[0])
        last_updated=date2str(df.index[-1])
        if date_compare(last_updated,first_updated)<minlen: return False
        if date_compare(last_updated,end)<0: return False
        # if date_compare(first_updated,start)>0: return False
        # print(first_updated,last_updated,len(df))
        return first_updated+' '+last_updated

    def get_listings(self):
        if os.path.exists(os.path.join(self.path,'listings.json')):
            with open(os.path.join(self.path,'listings.json'),'r') as f:
                return json.load(f)
        with open(os.path.join(self.path,'metadata.json'),'r') as f:
            metadata=json.load(f)
        listings={}
        count={}
        rep=0
        for md in metadata: 
            ret=self.check_listing(md)
            if not ret: continue
            code=get_code(md)
            if code not in count: count[code]=0
            count[code]+=1
            if code in listings: 
                rep+=1
                code+='.'+int_to_letters(count[code])
            listings[code]={'name':md,'metadata':metadata[md], 'span':ret}
        print(f'Listing created, length {len(listings)}, repeat {rep}')
        with open(os.path.join(self.path,'listings.json'),'w') as f:
            json.dump(listings,f)
        return listings
    
    def read(self,fname):
        path=os.path.join(self.path,'data',fname+'.csv')
        df=pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna(subset=['value'])
        df = df.groupby('date')['value'].agg(lambda x: x[x != 0].mean()).reset_index() # 0 are usually bug
        df.set_index('date',inplace=True)
        df.index = pd.to_datetime(df.index)
        df=df.sort_index()
        return df

    def query(self,q):
        if q not in self.listings: return ERRUNF+' queried symbol not in listing'
        df=self.read(self.listings[q]['name'])
        if self.interpolate:
            bottom=df['value'].values.min()*0.8
            interp_func = interp1d(df.index.astype(np.int64), df['value'], kind='cubic')
            ndate = pd.date_range(start=date2str(df.index[0]), end=date2str(df.index[-1]), freq='D')
            nvalue = interp_func(ndate.astype(np.int64))
            nvalue[nvalue < 0] = bottom
            df = pd.DataFrame({'date': ndate, 'value': nvalue})
            df.set_index('date',inplace=True)
            df.index = pd.to_datetime(df.index)
            df=df.sort_index()
        return df

def test_fte(root):
    print('|----------Test FTE Probe----------|')
    fte=FTE(root)
    df=fte.query('RP')
    print(df)
    print('|----------FTE Probe Pass----------|')
    print()


def parse_date_wo_decimal(date_string):
    return pd.to_datetime(date_string.split('.')[0])

class YG: # Opinions
    def __init__(self,root,interpolate=True):
        self.path=os.path.join(root,'Corpus','cache','YouGov')
        self.interpolate=interpolate

        if not os.path.exists(os.path.join(self.path,'metadata.json')):
            self.construct_time_series()
        self.listings=self.get_listings()

    def construct_time_series(self):
        if not os.path.exists(os.path.join(self.path,'data')):
            os.makedirs(os.path.join(self.path,'data'))
        with open(os.path.join(self.path,'trackings_metadata.json'),'r') as f:
            tmd=json.load(f) # this is provided by scraper
        metadata={}
        for xlsx in tmd:
            df = pd.read_excel(os.path.join(self.path,'trackings',xlsx))
            question=df.columns[0]
            date=df.columns[1::].tolist()
            index_qm = question.find("?")
            question= question[:index_qm+1] if index_qm != -1 else question
            for i in range(len(df)-2):
                row=df.loc[i].to_list()
                ndf={}
                title=tmd[xlsx]['title']+f'+{row[0]}'
                ndf['value']=row[1::]
                ndf['date']=date
                for m in [':','?']: title=title.replace(m,'')
                for m in [' ','/']: title=title.replace(m,'_')
                ndf=pd.DataFrame(ndf).set_index('date')
                ndf.to_csv(os.path.join(self.path,'data',title+'.csv'))
                metadata[title]={'title':tmd[xlsx]['title'],'question':tmd[xlsx]['question']}
        with open(os.path.join(self.path,'metadata.json'),'w') as f:
            json.dump(metadata,f)


    def read(self,fname):
        path=os.path.join(self.path,'data',fname+'.csv')
        df = pd.read_csv(path, parse_dates=['date'], date_parser=parse_date_wo_decimal)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna(subset=['value'])
        df = df.groupby('date')['value'].agg(lambda x: x[x != 0].mean()).reset_index() # 0 are usually bug
        df.set_index('date',inplace=True)
        df.index = pd.to_datetime(df.index)
        df=df.sort_index()
        return df
    
    def check_listing(self,md,end='2021-11-01',minlen=28):
        df=self.read(md)
        if len(df)<3: return False
        first_updated=date2str(df.index[0])
        last_updated=date2str(df.index[-1])
        if date_compare(last_updated,first_updated)<minlen: return False
        if date_compare(last_updated,end)<0: return False
        # if date_compare(first_updated,start)>0: return False
        # print(first_updated,last_updated,len(df))
        return first_updated+' '+last_updated
    
    def get_listings(self):
        if os.path.exists(os.path.join(self.path,'listings.json')):
            with open(os.path.join(self.path,'listings.json'),'r') as f:
                return json.load(f)
        with open(os.path.join(self.path,'metadata.json'),'r') as f:
            metadata=json.load(f)
        listings={}
        count={}
        rep=0
        for md in metadata: 
            ret=self.check_listing(md)
            if not ret: continue
            code=get_code(md,seg=True)
            if code not in count: count[code]=0
            count[code]+=1
            if code in listings: 
                rep+=1
                code+='.'+int_to_letters(count[code])
            listings[code]={'name':md,'metadata':metadata[md],'span':ret}
        print(f'Listing created, length {len(listings)}, repeat {rep}')
        with open(os.path.join(self.path,'listings.json'),'w') as f:
            json.dump(listings,f)
        return listings
    
    def query(self,q):
        if q not in self.listings: return ERRUNF+' queried symbol not in listing'
        df=self.read(self.listings[q]['name'])
        if self.interpolate:
            bottom=df['value'].values.min()*0.8
            interp_func = interp1d(df.index.astype(np.int64), df['value'], kind='cubic')
            ndate = pd.date_range(start=date2str(df.index[0]), end=date2str(df.index[-1]), freq='D')
            nvalue = interp_func(ndate.astype(np.int64))
            nvalue[nvalue < 0] = bottom
            df = pd.DataFrame({'date': ndate, 'value': nvalue})
            df.set_index('date',inplace=True)
            df.index = pd.to_datetime(df.index)
            df=df.sort_index()
        return df

def test_yg(root):
    print('|----------Test YG Probe----------|')
    yg=YG(root)
    df=yg.query('WTBP.SBD')
    print(df)
    print('|----------YG Probe Pass----------|')
    print()



def load_csv(key, dir):
    df = pd.read_csv(pjoin(dir, translate_forbidden_chars(key) + '.csv'))
    df.set_index('datetime', inplace=True)
    try:
        df.index = pd.to_datetime(df.index, utc=True, unit='ms')
    except:
        df.index = pd.to_datetime(df.index, utc=True)
    df=df.sort_index()
    return key, df

def parallel_load_icodes(root,parallel=True):
    dir = pjoin(root, 'Corpus', 'TS', 'ICode_dfs')
    keys = get_icodes(root)
    idfs = {}
    if parallel:
        pool = ProcessPool()
        results = pool.map(lambda key: load_csv(key, dir), keys)
        pool.close()
        pool.join()
        for key, df in results:
            idfs[key] = df
        pool.close()
    else:
        for key in keys:
            _, idfs[key] = load_csv(key, dir)
    return idfs

class OneProbe:
    """
    Query: HEAD:QUERY, UTC TIME
    Return: prior dataframe or error code, ***ALL DF MUST BE UTC TIME-VALUE***
    Notes:
    * For FTE, if a data is long time unupdated, then regard decayed to 0 
    * A query returns either date-value or ERROR:CODE
    """
    def __init__(self,root,apikeys,sysddir=None,cache=True,interpolate=True,offline=True):
        self.offline=offline # default use icode
        if self.offline:
            self.idfs=parallel_load_icodes(root)
            self.probe=self.iprobe
            self.metadata=load_json(pjoin(root, 'Corpus', 'TS', 'metadata.json'))
            for i in Buggy:
                del self.metadata[i]
        else:
            self.gt=GT(root,cache=cache,interpolate=interpolate,sysddir=sysddir)
            self.yf=YF(root,cache=cache)
            self.fred=FRED(root,apikeys,cache=cache)
            self.fte=FTE(root,interpolate=interpolate)
            self.yg=YG(root,interpolate=interpolate)

    def get_metadata(self,q): 
        if q not in self.metadata: return ERRUNF+f' icode {q} not found in ICode list.'
        return self.metadata[q]

    def iprobe(self,q,t,weeks=52,check=True,simplify=60):
        time = pd.to_datetime(t, utc=True)
        if q not in self.idfs: return ERRUNF+f' icode {q} not found in ICode list.'
        ts=self.idfs[q]
        prior = ts[ts.index <= time]
        if weeks is not None:
            begin=time-datetime.timedelta(weeks=weeks)
            prior=prior[begin<=prior.index]
        if check:
            if len(prior)==len(ts): return ERRINV+' no future data found for evaluation'
            if len(prior)==0: return ERRINV+' no prior data found'
        if simplify>0: return self.simplify(prior,simplify)
        return prior
    
    def simplify(self,prior,size=50):
        num_rows = len(prior)
        if num_rows <= size:
            new_df = prior
        else:
            step_size = num_rows // size
            new_df = prior.iloc[::step_size, :][-size:]
        return new_df


    def query(self,q,head,t):
        if not head in PROBECODE: return ERRDOM+' no such domain for probe, probe domain codes: '+str(PROBECODE)
        domain=PROBECODE[head]
        if domain in ['web','images','news','youtube','froogle']:
            ts=self.gt.query(q,t,domain) 
        elif domain in ['price']: #,'volume']: # volume is unstable, not a good ts to predict
            ts=self.yf.query(q.upper(),t,domain)
        elif domain in ['fred']:
            ts=self.fred.query(q.upper(),t)
        elif domain in ['fte','yg']:
            ts = eval(f"self.{domain}.query(q.upper())")            
        return ts

    def probe(self,q,t,weeks=52,check=True,simplify=50): # t is a UTC datetime, span: how many weeks history
        head,query=q.split(':', 1)
        time = pd.to_datetime(t, utc=True)
        ts=self.query(query,head,time)
        if 'ERROR' in ts: return ts
        if ts['value'].values.min()<=0: 
            ts['value']=ts['value']-ts['value'].values.min() # handle neg, lift up
            if len(ts[ts['value'] > 0])==0: return ERRINV+' all zeros time series'
            bottom = ts[ts['value'] > 0]['value'].values.min() # replace 0 by min
            ts['value'] = ts['value'].apply(lambda x: x if x > 0 else bottom)
        ts.index = pd.to_datetime(ts.index,utc=True)
        prior = ts[ts.index <= time]
        if weeks is not None:
            begin=time-datetime.timedelta(weeks=weeks)
            prior=prior[begin<=prior.index]
        if check:
            if len(prior)==len(ts): return ERRINV+' no future data found for evaluation'
            if len(prior)==0: return ERRINV+' no prior data found'
        # latest = prior.tail(1)['value'].values[0]
        # values=ts['value'].values.tolist()
        # npv = [num for num in values if num <= 0]
        # latest_time=latest.index.to_pydatetime()[0]
        # freshness=date_compare(d,date2str(latest_time))
        # Handle 0
        # if latest<=0 or len(npv)>0: # pre filtered out during scan
        #     return ERRINV+' value invalid, less than or equal to 0'
        if simplify>0: return self.simplify(prior,simplify)
        return prior


def test_op(root,apikeys):
    print('|----------Test One Probe----------|')
    op=OneProbe(root,apikeys)
    
    date="2023-02-01"
    from dateutil import parser
    date = parser.parse('2023-02-02 7:40 PM ET', tzinfos={"ET": "US/Eastern"})

    # q='FTE:DP' # FIN:AAPL FRD:GDP FTE:DP FOEC.NS.B YGV:FOEC.NS.B
    # # ts=op.query(q,domain)
    # ret=op.probe(q,date)
    
    q='FRD:GDP' # FIN:AAPL FRD:GDP FTE:DP FOEC.NS.B YGV:FOEC.NS.B
    ret=op.probe(q,date,check=False)
    # print(ret)
    ret=op.probe('FRD:CPI',date,check=False)
    ret=op.probe('FRD:PPI',date,check=False)


    # print(ret)
    print('|----------One Probe Pass----------|')
    print()


