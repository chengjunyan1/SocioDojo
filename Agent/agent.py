import openai
import random
import sys,os
sys.path.append(".")  # Add the parent directory to sys.path
import numpy as np

# from config import history_span,root,wiki_app_name,wiki_api_token,fred_api_key,name_email

# from Agent.const import DCODE_DESCRIPTION

from Agent.utils import pjoin,pexist,makedirs,save_json,load_json,get_icodes
from Agent.actuator import ChatActuator
from Agent.assistant import ChatAssistant
from Agent.analyst import ChatAnalyst


def build_agent(root,savename,apikeys,config,debug_mode=False):
    agenttype=config['type']
    agentname=config['name']
    agentconfig=config['config']
    if agenttype=='random':
        return RandomAgent(agentconfig,root,savename,agentname,apikeys)
    elif agenttype=='chatagent':
        return ChatAgent(agentconfig,root,savename,agentname,apikeys,debug_mode)
    else:
        raise f'Agent type {agenttype} not found.'


class BaseAgent:
    def __init__(self,root,savename,agentname,agenttype,apikeys):
        self.root=root
        self.apikeys=apikeys
        self.savename=savename # name of the save folder
        self.agenttype=agenttype # name of the agent
        self.savepath=os.path.join(self.root,'Ckpts',savename,'Agent',agentname)
        makedirs(self.savepath)

        self.bindpoints=['probe','trade','query','state','listen','get_metadata'] # basic bindpoints
        self.endpoints={
            'save': self.save,
            'load': self.load,
            'sense': self.sense,
        } # can add more endpoints
        self.registry=None

    def register(self,register_entry): # input the register entry of a world
        endpoints=register_entry(self.endpoints)
        self.registry={}
        for i in self.bindpoints: 
            if i not in endpoints: raise f"{i} not provided in endpoints by world"
            self.registry[i]=endpoints[i]
        self.setup()

    def setup(self): # will be called register
        raise NotImplementedError

    def save(self): pass

    def load(self): pass
    
    def sense(self,message): # passive
        raise NotImplementedError
    
    def watch(self,symbols): # active
        raise NotImplementedError
    
    def query(self,q,data=None): return self.registry['query'](q,data)    
    
    def probe(self, q, info=None):
        if info is None:
            info=True if q[:4]=='FIN:' else False
        res=self.registry['probe'](q,info)  
        if 'ERROR' in res: return res
        if q[:4]=='FIN:': 
            if 'information' in res:
                del res['information']['info']  # not needed, icode info already cover
                if len(res['information'])==1: del res['information']
        return res
    
    def get_metadata(self,icode):
        return self.registry['get_metadata'](icode)

    def trade(self,icode,amount):
        ret=self.registry['trade'](q=icode,amount=amount)
        return ret['result']
    
    def state(self,detailed=False): # detailed: whether include transactions history
        return {'account':self.registry['state'](detailed=detailed)}

    def listen(self,channels): self.registry['listen'](channels)



class RandomAgent(BaseAgent):
    def __init__(self,config, root, savename,agentname,apikeys):
        super().__init__(root, savename,agentname, 'random',apikeys)
        self.config=config
        self.wl=None # only use one wl
        self.icode=get_icodes(root)
        # self.convert_et=get_convert_et(root)

    def sense(self, message):
        if self.registry is None: raise "Not registered with any world"
        ret=message['news']
        report=self.state()['account']
        total=report['total value']
        # if total<1: self.query('SYS:quit','bankrupt')
        assets=report['portfolios']['default']
        holdings=[]
        for i in assets:
            if i not in ['total','recent transactions']: 
                holdings.append(i)

        amount=np.random.randint(-10000,10000)
        if np.random.rand()<0.5: amount=0
        if amount>0:
            amount=np.random.randint(5000,50000)
            icode=random.choice(self.icode)
        elif amount<0:
            if len(holdings)==0:
                amount=0
            else:
                asset=random.choice(holdings)
                print(assets)
                amount=-np.random.rand()*assets[asset]['value']
                icode=asset[:-2]

        if isinstance(ret,list):
            dts=[(i['datetime'],i['source']) for i in ret]
            print(dts,total)
        else: print(ret['datetime'],ret['source'],total)
        if amount!=0: 
            ret=self.trade(icode,amount)
            print('Move:',icode,amount,ret)

    def setup(self):
        self.listen(self.config['channels'])
        self.wl=self.registry['query']('SYS:watchlist',{
            'id':'wl1','symbols':[],'levels':self.config['wl_levels']})
        
    def save(self): 
        state={}
        if self.wl is not None:
            state['wl_symbols']=self.wl.symbols
        save_json(pjoin(self.savepath,'state.json'),state)

    def load(self): 
        state=load_json(pjoin(self.savepath,'state.json'))
        if 'wl_symbols' in state:
            self.wl.add_symbols(state['wl_symbols'])
        


class ChatAgent(BaseAgent):
    """
    Should only expose chat to agent
    """
    def __init__(self,config,root,savename,agentname,apikeys,debug_mode=False):
        super().__init__(root,savename,agentname,'chatworld',apikeys)
        self.config=config
        self.ruleset=config['ruleset']
        self.debug_mode=debug_mode
        self.savedir=pjoin(self.savepath,'chat')
        makedirs(self.savedir)
        
        self.record_index=0
        while True:
            if pexist(pjoin(self.savedir,f'{self.record_index}.json')) or \
                pexist(pjoin(self.savedir,'unread',f'{self.record_index}.json')) or \
                pexist(pjoin(self.savedir,'unsend',f'{self.record_index}.json')): 
                self.record_index+=1
                continue
            break
        

    def setup(self):
        print('Setup Actuator...')
        self.actuator=ChatActuator(self.root,self.trade,self.state,self.probe,self.get_metadata, self.apikeys['openai_apikey'],
                                   model_name=self.config['assistant_model'],verbose=self.config['actuator_verbose'],
                                   limit=self.config['actuator_limit'],ruleset=self.ruleset,
                                   temperature=self.config['temperature'],top_p=self.config['top_p'],)
        print('Setup Assistant...')
        self.assistant=ChatAssistant(self.root,self.query,self.probe,self.actuator.query,self.get_metadata,
                                     self.apikeys['openai_apikey'],model_name=self.config['assistant_model'],
                                     verbose=self.config['assistant_verbose'],limit=self.config['assistant_limit'],
                                     temprature=self.config['temperature'],top_p=self.config['top_p'])
        print('Setup Analyst...')
        self.analyst=ChatAnalyst(self.actuator,self.assistant,self.apikeys['openai_apikey'],model_name=self.config['analyst_model'],
                                 verbose=self.config['analyst_verbose'],limit=self.config['analyst_limit'],debug_mode=self.debug_mode,
                                 ruleset=self.ruleset,analyse_fn=self.config['analyse_fn'],second_response=self.config['second_response'],
                                 serp_apikey=self.apikeys['serp_apikey'],temprature=self.config['temperature'],top_p=self.config['top_p'])
    
    def sense(self, message):
        record=self.analyst(message) 
        time=str(message["time"])
        record['time']=time
        if not record['read']: sdir=pjoin(self.savedir,'unread')
        else:
            if not record['send']: sdir=pjoin(self.savedir,'unsend') 
            else: sdir=self.savedir
        makedirs(sdir)
        save_json(pjoin(sdir,f'{self.record_index}.json'),record)
        self.record_index+=1

