import copy
import time
import uuid
import os,shutil

from Env.world import build_world
from Agent.agent import build_agent
from Env.utils import pexist,pjoin,makedirs,save_json,load_json

from config import sawi_worldconfig,base_config,base_agentconfig



def build(config,debug_mode=False):
    root=config['root']
    savename=config['savename']
    apikeys=config['apikeys']
    confdir=pjoin(root, 'Ckpts', savename,'config.json')
    makedirs(pjoin(root, 'Ckpts', savename))
    save_json(confdir,config)
    agentconfig=config['agentconfig'] # can be multi config if it is multiagent
    worldconfig=config['worldconfig']
    configtype=config['type']
    t0=time.time()
    print('Start building...')
    if configtype=='sawi':
        ret=SAWIBuilder(root,savename,apikeys,agentconfig,worldconfig,debug_mode)
    else:
        raise f'Config type {configtype} not found.'
    print(f'System built, time: {time.time()-t0}')
    return ret

def load(root,savename):
    confdir=pjoin(root, 'Ckpts', savename,'config.json')
    config=load_json(confdir)
    if config=={}: raise f'Checkpoint {savename} not found in {root}/Ckpts'
    return build(config)


class Builder:
    def __init__(self,root,savename,apikeys,debug_mode=False):
        self.apikeys=apikeys
        self.root=root
        self.savename=savename
        self.path=pjoin(root,'Ckpts',savename)
        self.debug_mode=debug_mode

    def run(self,end_time=None):
        return self.world.run(end_time,self.debug_mode)


class SAWIBuilder(Builder):
    def __init__(self, root,savename,apikeys,agentconfig,worldconfig,debug_mode=False):
        super().__init__(root,savename,apikeys,debug_mode)
        t0=time.time()
        self.world=build_world(root,savename,apikeys,worldconfig)
        print(f'World built, spent {time.time()-t0:.2f} secs')
        t0=time.time()
        self.agent=build_agent(root,savename,apikeys,agentconfig,debug_mode)
        print(f'Agent built, spent {time.time()-t0:.2f} secs')
        t0=time.time()
        self.agent.register(self.world.register)
        print(f'Registered, spent {time.time()-t0:.2f} secs')


def test_sawi(savename='test',ruleset=[]):
    print()
    print(f'Runing test {savename}...')
    config=copy.deepcopy(base_config)
    root=config['root']
    if pexist(pjoin(root,'Ckpts',savename)):
        shutil.rmtree(pjoin(root,'Ckpts',savename))
    config['ruleset']=[]
    worldconfig=copy.deepcopy(sawi_worldconfig)
    worldconfig['default_channels']=['WSJ']#,'WSJ','NYT']
    worldconfig['period']=0 #300
    worldconfig['ruleset']=config['ruleset']
    config['worldconfig']=worldconfig
    agentconfig=copy.deepcopy(base_agentconfig)
    agentconfig['type']='random'
    agentconfig['name']='default'
    agentconfig['config']={
        'channels':{'WSJ':None},  #{'NYT':None,'WSJ':None,'TTT':None},
        'wl_levels':{'FIN':0.04,'default':0.05,'FRD':0.02},
        'ruleset': config['ruleset'],
    }

    config['agentconfig']=agentconfig
    config['savename']=savename
    config['type']='sawi'
    run=build(config)
    end_time='2023-08-01'
    report=run.run(end_time)
    return report


def run_ca(savename=None,analyse_fn='',debug_mode=False):
    if savename is None: 
        savename=uuid.uuid4()
    print(f'Runing {savename}...')
    config=copy.deepcopy(base_config)
    config['ruleset']=['nodt']
    worldconfig=copy.deepcopy(sawi_worldconfig)
    worldconfig['default_channels']=['WSJ']
    worldconfig['period']=0 #300
    worldconfig['ruleset']=config['ruleset']
    config['worldconfig']=worldconfig

    agentconfig=copy.deepcopy(base_agentconfig)
    agentconfig['type']='chatagent'
    agentconfig['name']='default'
    agentconfig['config']={
        'channels':{'WSJ':None},  
        'analyst_limit': 4,
        'actuator_limit': 4,
        'assistant_limit': 2,
        'temperature': 0.2,
        'top_p': 0.1,
        'analyst_model': 'gpt-3.5-turbo-16k',
        'actuator_model': 'gpt-3.5-turbo-16k',
        'assistant_model': 'gpt-3.5-turbo-16k',
        'analyst_verbose': True,
        'actuator_verbose': False,
        'assistant_verbose': False,
        'ruleset': config['ruleset'],
        'analyse_fn': analyse_fn,
        'second_response': True,
    }

    config['agentconfig']=agentconfig
    config['savename']=savename
    config['type']='sawi'
    run=build(config,debug_mode)
    end_time='2023-08-01'
    run.run(end_time)

