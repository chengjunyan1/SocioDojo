import uuid
import os,json
from tqdm import tqdm

import openai
from langchain.docstore.document import Document
from langchain.embeddings.openai import OpenAIEmbeddings
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from langchain.embeddings import HuggingFaceInstructEmbeddings

try:
    from Agent.utils import pexist,pjoin,readtxt,load_json,save_json
    import Agent.prompts.assistant_instruct as PROMPT
except:
    from utils import pexist,pjoin,readtxt,load_json,save_json
    import prompts.assistant_instruct as PROMPT


class KBDB:
    """
    Query: input query sentence, output N candidates and their icodes for the agent to choose
    """
    def __init__(self,root,load=True,reset=False):
        self.root=root
        self.dbdir=pjoin(root,'Agent','db')
        self.client = chromadb.PersistentClient(path=self.dbdir, settings=Settings(allow_reset=True))
        # self.client.reset()
        self.db=self.get_kb_collection(load,reset)

    def get_kb_collection(self,load=True,reset=False):
        corpus_instruct='Represent the article snippet from a knowledge base consisted of textbooks, journals, and encyclopedias, for retrieval: '
        query_instruct='Represent the query for retrieving relevent article snippets from a knowledge base consisted of textbooks, journals, and encyclopedias: '
        print('Current collections',self.client.list_collections())
        collection_name='KB_split'
        if load and collection_name in [i.name for i in self.client.list_collections()]:
            if reset: 
                self.client.delete_collection(collection_name)
                print(F'Delete {collection_name} collection for reset, current collections:',self.client.list_collections())
            else:
                emb_ef = embedding_functions.InstructorEmbeddingFunction(
                    model_name="hkunlp/instructor-large",device='cuda',instruction=query_instruct)
                collection = self.client.get_collection(collection_name,embedding_function=emb_ef)
                print(F'Found existing {collection_name} collection with length',collection.count())
                return collection
        emb_ef = embedding_functions.InstructorEmbeddingFunction(
            model_name="hkunlp/instructor-large",device='cuda',instruction=corpus_instruct)
        collection = self.client.get_or_create_collection(collection_name,embedding_function=emb_ef)
        print(f'Collection count: {collection.count()}')

        kb=load_json(pjoin(self.root,'Corpus','KB','KB_split.json'))
        print(f'Length of KB: {len(kb)}')
        # keys,values=[],[]
        check=True
        for key,value in tqdm(kb.items()):
            # keys.append(key)
            # values.append(value)
            if check and len(collection.get(ids=key)['ids'])>0: continue
            else: check=False # no need to check anymore
            collection.upsert(ids=key,documents=value)
        return collection
    
    def query(self,query,n_results=5):
        return self.db.query(query_texts=query,n_results=n_results)


class BaseAssistant:
    """
    Retrive evidences to support the proposition from analyst
    Args:
        query_fn (ep: query): function to query the tools, mainly internet search engines
        probe_fn (ep: probe): function to probe the historical time series and related info through broker
    """
    def __init__(self,root,query_fn,probe_fn,query_icode,get_metadata,openai_apikey,limit=5):
        self.root=root
        self.query_fn=query_fn
        self.probe_fn=probe_fn
        self.query_icode=query_icode # str -> str
        self.get_metadata=get_metadata # str -> dict: name, info
        self.limit=limit
        os.environ["OPENAI_API_KEY"] = openai_apikey
        openai.api_key=openai_apikey
        self.db=KBDB(root)

    def ask(self,query,time,limit=5):
        raise NotImplementedError

    def __call__(self,query,time,data=None):
        messages,content=self.ask(query,time,self.limit)
        return content,messages



class ChatAssistant(BaseAssistant):
    def __init__(self,root,query_fn,probe_fn,query_icode,get_metadata,openai_apikey,
                 model_name="gpt-3.5-turbo-16k",verbose=False,limit=5,temprature=0.2,top_p=0.1):
        super().__init__(root,query_fn,probe_fn,query_icode,get_metadata,openai_apikey,limit)
        self.model_name=model_name
        self.verbose=verbose
        self.temprature=temprature
        self.top_p=top_p
    
    def askdb(self,query,n_results=5):
        return self.db.query(query,n_results=n_results)
    
    def wikisearch(self,query): return self.query_fn('WKI:'+query)
    def googlesearch(self,query): return self.query_fn('GGL:'+query)
    def gkgsearch(self,query): return self.query_fn('GKG:'+query)

    def probe(self,q,info=None): return self.probe_fn(q,info)  
    
    def message(self,role,content,name=None):
        m={'role':role, 'content':content}
        if name is not None: m['name']=name
        if self.verbose: self.show_message(m)
        return m
    
    def show_message(self,message):
        role=message["role"]
        print(f'[{role}]:\n\n{message["content"]}\n\n')

    def handle_query(self,message,messages): # search api already set time in world
        done=False
        query=None
        if "function_call" in message:
            fn=message["function_call"]['name'].lower()
            ok=False
            try:
                args=json.loads(message["function_call"]['arguments'])
                ok=True
            except: message=self.message('system',f'ERROR: Arguments {message["function_call"]["arguments"]} is not a json. You must provide a json as arguments.')
            if ok:
                if fn=='done': 
                    done=True
                    message=self.message('system','You have finished the query.')
                elif fn=='wikisearch': 
                    ret=self.wikisearch(args['query'])
                    query=args['query']
                elif fn=='googlesearch': 
                    ret=self.googlesearch(args['query'])
                    query=args['query']
                elif fn=='gkgsearch': 
                    ret=self.gkgsearch(args['query'])
                    query=args['query']
                elif fn=='probe': 
                    ret=self.probe(args['icode'])
                    query=args['icode']
                elif fn=='askdb': 
                    ret=self.askdb(args['query'])
                    query=args['query']
                elif fn=='query_icode': 
                    ret=self.query_icode(args['query'])
                    query=args['query']
                elif fn=='get_metadata':
                    ret=self.get_metadata(args['icode'])
                    query=args['icode']
                else: message=self.message('system',f'Invalid function call: {fn}. Valid function calls are wikisearch, googlesearch, gkgsearch, probe, askdb, query_icode.')
        else: message=self.message('system','You do not call any function. If you have finished the query, please call done function.')
        if query is not None: 
            messages.append(self.message('system',f'You are querying {fn}: {query}'))
            message=self.message('function',f'Return for your query {query}:'+str(ret),fn)
        messages.append(message)
        if query is not None: 
            messages.append(self.message('system','Now based on the results, accomplish the query. If there is an error such as wrong ICode when calling the probe, handle it and retry. You can also do futher search or decompose the query. When you finish, call done function.'))
        return done,messages

    def ask(self,query,time,limit=5):
        messages=[self.message('system',PROMPT.context.format(query=query,time=time))]
        while limit>0:
            limit-=1
            response = openai.ChatCompletion.create(
                model=self.model_name,
                messages=messages,
                functions=PROMPT.search_functions,
                function_call="auto", 
                temprature=self.temprature,
                top_p=self.top_p
            )
            message = response["choices"][0]["message"]
            content=message["content"]
            done,messages=self.handle_query(message,messages)
            if done: break
        messages.append(self.message('system',f'Now, based on the search results, give your final reply to the query from analyst "{query}". You must clearly reference from the search results returned by function calls.'))
        output = openai.ChatCompletion.create(
            model=self.model_name,
            messages=messages,
            temprature=self.temprature,
            top_p=self.top_p
        ) 
        message=output["choices"][0]["message"]
        content=message["content"]
        messages.append(message)
        if self.verbose: self.show_message(message)
        return messages,content
        
        

