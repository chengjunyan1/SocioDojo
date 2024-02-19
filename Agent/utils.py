import os,io
import json
import re

from serpapi import GoogleSearch
from fredapi import Fred

try:
    from .const import Buggy
except:
    from const import Buggy



pjoin=os.path.join
pexist=os.path.exists

def save_json(path,jsf):
    with open(path, 'w') as f:
        json.dump(jsf,f)

def load_json(path, default={}):
    if os.path.exists(path):
        with open(path, 'r') as f:
            jsf=json.load(f)
        return jsf
    else: return default
    
def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

def readtxt(path):
    with open(path) as f:
        return f.read()

def get_icodes(root):
    with open(pjoin(root,'Corpus','TS','icode_list.txt'),'r',encoding='utf-8') as f:
        icodes=[i.strip() for i in f.readlines()]
        icodes=[i for i in icodes if i not in Buggy]
        return icodes
        
def parse_md(md):
    parsed=''
    for i in md:
        if md[i]=="": continue
        if isinstance(md[i],str) and is_url(md[i]): continue
        # if isinstance(md[i],list):
        #     use=False
        #     term=''
        #     term+=f'{i}: '
        #     for j in md[i]:
        #         if isinstance(j,str) and not is_url(j): use=True
        #         term+=f'{j}, '
        #     term=term[:-2]+'\n'
        #     if use: parsed+=term
        parsed+=f'{i}: {md[i]}\n'
    return parsed

def is_url(string):
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return re.match(regex, string) is not None

def get_tracklist(root):
    with open(pjoin(root,'Corpus','TS','tracklist.txt'),'r',encoding='utf-8') as f:
        return [i.strip() for i in f.readlines()]


def google(question,serpapi_key,datetime):
    dt=datetime.strftime('%m/%d/%Y')
    params = {
        "api_key": serpapi_key,
        "engine": "google",
        "q": question,
        "google_domain": "google.com",
        "gl": "us",
        "hl": "en",
        "tbs": f"cdr:1,cd_max:{dt}"
    }
    with io.capture_output() as captured: #disables prints from GoogleSearch
        search = GoogleSearch(params)
        res = search.get_dict()
    if 'answer_box' in res.keys() and 'answer' in res['answer_box'].keys():
        toret = res['answer_box']['answer']
    elif 'answer_box' in res.keys() and 'snippet' in res['answer_box'].keys():
        toret = res['answer_box']['snippet']
    elif 'answer_box' in res.keys() and 'snippet_highlighted_words' in res['answer_box'].keys():
        toret = res['answer_box']["snippet_highlighted_words"][0]
    elif 'snippet' in res["organic_results"][0].keys():
        toret= res["organic_results"][0]['snippet'] 
    else:
        toret = None
    return toret





