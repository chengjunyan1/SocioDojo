import os,time
import functools as ft
import pandas as pd
from datetime import timedelta
import numpy as np

from Env.interfaces.probe import OneProbe
from Env.interfaces.query import UniQuery
from Env.interfaces.watch import AllWatch
from Env.modules.trade import Broker
from Env.modules.info_mod import WatchList
from Env.const import ERREMP,SYSQUIT,ERRINV,QUERYCODE
from Env.utils import pjoin,pexist,get_icodes,utc2str,load_json,save_json,makedirs,str2utc




def build_world(root,savename,apikeys,config):
    worldtype=config['type']
    init_time=config['init_time']
    sysddir=config['sysddir']
    num_results=config['num_results']
    default_channels=config['default_channels']
    period=config['period']
    ruleset=config['ruleset']
    if worldtype=='sawi':
        return SingleAgentWorldICode(savename,root,init_time,apikeys,sysddir,period,num_results,
                                     default_channels=default_channels,ruleset=ruleset)
    else:
        raise f'World type {worldtype} not found'



class WorldEnv:
    """
    Run the world, feeds AW, expose OP, UQ to agent as state and prior, run a loop 
    """
    def __init__(self,root,init_time,apikeys,sysddir=None,offline=True,num_results=3,
                 cache=True,interpolate=True,default_channels=['NYT','WSJ'],ruleset=[]):
        self.time = pd.to_datetime(init_time, utc=True)
        self.ruleset=ruleset

        t0=time.time()
        self.op=OneProbe(root,apikeys,sysddir,cache,interpolate,offline)
        print(f'Probe loaded, spent {time.time()-t0}')
        self.uq=UniQuery(root,apikeys,num_results,cache)
        t0=time.time()
        self.aw=AllWatch(root,self.time,default_channels)
        print(f'Channels loaded, spent {time.time()-t0}')

        self.endpoints={} # provide exposed interfaces here


    
def filter_by_category(ret):
    metadata=ret['metadata']
    source=ret['source']
    if source=='WSJ': 
        category=metadata['category']
        # whitelist=['WORLD','BUSINESS','U.S.','POLITICS','ECONOMY','TECH','FINANCE']
        whitelist=['WORLD','BUSINESS','U.S.','POLITICS','ECONOMY','TECH','FINANCE','OPINION']
        if category in whitelist: return True
        else: return False
    return True
    

class SingleAgentWorldICode(WorldEnv):
    """
    Hyper portfolio task, use ICode to query, no domain code
    Expose endpoints to agent, agent expose "listen" to it
    """
    def __init__(self,savename,root,init_time,apikeys,sysddir=None,period=0,num_results=3,cache=True,
                 interpolate=True,default_channels=['NYT','WSJ'],offline=True,ruleset=[]):
        super().__init__(root,init_time,apikeys,sysddir,offline,num_results,
                         cache,interpolate,default_channels,ruleset)
        
        self.broker=Broker(root,savename,init_time,self.uq,self.op,self.ruleset) # broker as the interface to data and market
        self.broker.register('default')
        self.period=period
        self.residual=period # seconds
        self.watch=[] # interrupt channels
        self.path=os.path.join(root,'Ckpts',savename,'World')
        makedirs(self.path)

        # pass endpoints to agent when install
        self.endpoints={
            'probe': self.probe,
            'trade': ft.partial(self.broker.trade,account='default',hp='default'), # return {'result':str}
            'query': self.query,
            'state': ft.partial(self.broker.account_report,account='default'), # get current status, expose to agent, but usually passitively accept reports
            'listen': self.listen,
            'get_metadata': self.op.get_metadata, # get metadata of a symbol
        }
        self.icodes=get_icodes(root)

        self.registry=None          
        self.bindpoints=['save','load','sense']

    def register(self,endpoints):
        self.registry={}
        for i in self.bindpoints: 
            if i not in endpoints: raise f"{i} not provided in endpoints by agent"
            self.registry[i]=endpoints[i]
        return self.endpoints

    def probe(self,q,info=False): # directly input ICode, can also be used to check some data
        if q not in self.icodes: return ERRINV+' wrong ICode, not listed in ICode list'
        return self.broker.query(q,info)

    def query(self,q,data=None): # head:query 
        head,query=q.split(':', 1)
        if head=='SYS':
            if query=='time': return self.time
            elif query=='watch': self.watch=data # watch channels
            elif query=='watchlist': # watch symbols
                wl=WatchList(self.time,self.op,data['symbols'],data['levels'])
                wlid=data['id']
                self.aw.add_channel(wlid,self.time,(f'Watchlist {wlid}',wl))
                return wl
            elif query=='quit': self.syspipe=SYSQUIT+' '+data
        return self.uq.query(q,self.time,data) # PRB:QUERY, provide information
    
    def listen(self,channels): # e.g. {'wl1':('Watch List 1',WatchList obj)}
        self.aw.update_chennels(channels,self.time) # can also use it to add or del channels
    
    def _move(self,debug_mode): 
        if debug_mode:
            rand=np.random.randint(1,1000)
            print(f'Random move {rand} steps')
            for i in range(rand):
                ret=self.aw.pop()
                if ERREMP in ret: return ret
                self.time=ret[0]['datetime']
            return ret
        if self.period>0: # need test
            p_next=self.time+timedelta(seconds=self.period)
            t_move=self.residual if self.residual!=0 else self.period
            ret,t_next=self.aw.tpop(self.time,t_move,self.watch)
            if ERREMP in ret: return ret
            self.residual=(p_next-t_next).total_seconds()
            self.time=t_next if self.residual!=0 else p_next
        else:
            ret=self.aw.pop()
            if ERREMP in ret: return ret
            self.time=ret[0]['datetime']
        return ret

    def move(self,debug_mode,filter_fn=filter_by_category): # filter_fn: True if pass the filter 
        while True:
            ret=self._move(debug_mode)
            newret=[]
            for i in ret:
                if 'ERROR' in ret or filter_fn is None or filter_fn(i): 
                    newret.append(i) # error shown, or no filter, or any news pass the filter
            if len(newret)>0: break
        return ret

    def save(self):
        self.broker.save()
        self.registry['save']()
        state={ # mainly time, and agent's watching
            'time': utc2str(self.time),
            'watch': self.watch,
            'period': self.period,
            'residual': self.residual,
        }
        savedir=pjoin(self.path,'state.json')
        save_json(savedir,state)

    def load(self):
        self.broker.load()
        self.registry['load']()
        savedir=pjoin(self.path,'state.json')
        state=load_json(savedir)
        if state=={}: return False
        self.time=str2utc(state['time'])
        self.aw.settime(self.time)
        self.period=state['period']
        self.residual=state['residual']
        self.watch=state['watch']
        return True

    def run(self,end_time=None,debug_mode=False):
        if self.registry is None: raise "No agent registered"
        if not debug_mode: self.load()
        while True:
            self.syspipe=None
            if end_time is not None:
                end_time=pd.to_datetime(end_time,utc=True)
                if self.time>=end_time: break
            ret=self.move(debug_mode)
            if self.syspipe is not None:
                if SYSQUIT in self.syspipe:
                    print(self.syspipe)
                    break
            if ERREMP in ret: break
            self.broker.settime(self.time) # setup broker first
            message={
                'time':self.time,
                'news':ret
            }
            self.registry['sense'](message)
            if not debug_mode: self.save()
            if debug_mode: break
        if not debug_mode: self.save()
        print(self.broker.report())
        return self.broker.report()




class MultiAgentWorld(WorldEnv):
    """
    # STILL WORKING #
    Multi agent world env
    World coordinate time, different agent may have different frequency of listening
    """
    def __init__(self,root,init_time,apikeys,sysddir=None,
                 num_results=3,cache=True,interpolate=True):
        super().__init__(root,init_time,apikeys,sysddir,
                        num_results,cache,interpolate)
        self.periods={} # seconds, drive world by time when period>0, otherwise by news
        self.listen_list={}

    def install(self,agent_name,listen_func): 
        if self.endpoints=={}: raise NotImplementedError
        self.listen_list[agent_name]=listen_func # pass json
        return self.endpoints
    
    def uninstall(self,agent_name):
        if agent_name in self.listen_list:
            del self.listen_list[agent_name]
            return True
        return False
    
    def move(self): 
        if self.period>0:
            ret,t_next=self.aw.tpop(self.time,self.period)
            self.time=t_next # FIXIT, multi-agent interrupt, multi period
        else:
            ret=self.aw.pop()
            if ERREMP in ret: return ret
            self.time=ret[0]['datetime']
        return ret

    def forward(self):
        # call move here
        raise NotImplementedError
    
    def handler(self,agent,response): # can custom system message here
        if response is None: return None
        if SYSQUIT in response:
            self.uninstall(agent)
        return None
    
    def before_loop(self): return
    def inloop_begin(self): return # before ret
    def inloop_middle(self,ret): return # after ret
    def inloop_end(self): return # after ret handled
    def after_loop(self): return 

    def run(self): # The world drive the time, agents communicate only when time match
        if self.endpoints=={}: raise NotImplementedError
        system_message={}
        self.before_loop()
        while True:
            if self.listen_list=={}: break
            self.inloop_begin()
            ret=self.forward()
            self.inloop_middle(ret)
            responses={}
            for agent in self.listen_list:
                message={
                    'time':self.time,
                    'news':ret,
                }
                if agent in system_message:
                    message['system']=system_message[agent]
                responses[agent]=self.listen_list[agent](message)
            system_message={}
            for agent in responses:
                response=responses[agent]
                handle=self.handler(agent,response) # handle response here
                if handle is not None:
                    system_message[agent]=handle
            self.inloop_end()
            if ERREMP in ret: break
        self.after_loop()


def test_maw(root,history_span,apikeys):
    print('|----------Test Single Agent World----------|')
    savename='test001'
    period=7200
    init_time='2023-7-31'
    maw=MultiAgentWorld(root,init_time,history_span,apikeys,period)

    def listen1(message):
        print(message['time'])
        print(len(message['news']),'articles')

    def listen2(message): 
        print('Listen 2')
        return SYSQUIT

    maw.install(listen_func=listen1)
    maw.run()

    print('|----------Single Agent World Pass----------|')
    print()
