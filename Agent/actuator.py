import uuid
import os
from tqdm import tqdm
import openai
import json

from typing import Optional

from langchain.chains.openai_functions import (
    create_openai_fn_chain,
    create_structured_output_chain,
)
from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, HumanMessagePromptTemplate
from langchain.schema import HumanMessage, SystemMessage
from langchain.memory import ConversationBufferMemory

import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions

try:
    from Agent.utils import pexist,pjoin,readtxt,get_icodes,get_tracklist
    import Agent.prompts.base_instruct as BASE_PROMPT
    import Agent.prompts.actuator_instruct as PROMPT
except: 
    from utils import pexist,pjoin,readtxt,get_icodes,get_tracklist
    import prompts.base_instruct as BASE_PROMPT
    import prompts.actuator_instruct as PROMPT


class ICodeDB:
    """
    Query: input query sentence, output N candidates and their icodes for the agent to choose
    """
    def __init__(self,root,reset=False):
        self.root=root
        self.dbdir=pjoin(root,'Agent','db')
        self.client = chromadb.PersistentClient(path=self.dbdir, settings=Settings(allow_reset=True))
        # self.client.reset()
        self.db=self.get_icb_collection(reset)

    def get_icb_collection(self,reset=False):
        corpus_instruct='Represent the description of a time series for retrieval: '
        query_instruct='Represent the query for retrieving relevent time series: '
        print('Current collections',self.client.list_collections())
        collection_name='ICodes'
        if collection_name in [i.name for i in self.client.list_collections()]:
            if reset: 
                self.client.delete_collection(collection_name)
                print(f'Delete {collection_name} collection for reset, current collections:',self.client.list_collections())
            else:
                emb_ef = embedding_functions.InstructorEmbeddingFunction(
                    model_name="hkunlp/instructor-large",device='cuda',instruction=query_instruct)
                collection = self.client.get_collection(collection_name,embedding_function=emb_ef)
                print(f'Found existing {collection_name} collection with length',collection.count())
                return collection
        emb_ef = embedding_functions.InstructorEmbeddingFunction(
            model_name="hkunlp/instructor-large",device='cuda',instruction=corpus_instruct)
        collection = self.client.create_collection(collection_name,embedding_function=emb_ef)
        print(f'Collection init: {collection.count()}')

        codebook=readtxt(pjoin(self.root,'Corpus','TS','ICodebook.txt'))
        codes = codebook.split('\n\n')
        print(f'Length of ICodebook: {len(codes)}, tokens ~{len(codebook)//4000}K')
        for i in tqdm(range(len(codes))):
            icode=codes[i].split('\n')[0].split(' ')[1]
            doc=codes[i].split('\n',1)[1].replace(' * This ICode can be used to access','This item is')
            collection.upsert(ids=icode,documents=doc,metadatas={'domain':icode.split(':')[0]})
        return collection
    
    def query(self,query,n_results=5):
        return self.db.query(query_texts=query,n_results=n_results)


class BaseActuator:
    """
    Actuator to execute the trade based on the analyst's analysis
    args:
        trade_fn (ep: trade): function to trade the assets
        state_fn (ep: state): function to get the current state of the assets including account, portfolio, etc.
    """
    def __init__(self,root,trade_fn,state_fn,probe_fn,get_metadata_fn,
                 openai_apikey,verbose=False,simu_mode=False,ruleset=[]):
        self.root=root
        self.trade_fn=trade_fn
        self.state_fn=state_fn
        self.probe_fn=probe_fn
        self.get_metadata_fn=get_metadata_fn
        self.icb=ICodeDB(root)
        os.environ["OPENAI_API_KEY"] = openai_apikey
        openai.api_key=openai_apikey
        self.verbose=verbose
        self.simu_mode=simu_mode
        self.ruleset=ruleset
        if 'track' in self.ruleset:
            tracklist=get_tracklist(root)
            self.tracklist={}
            for i in tracklist:
                try:
                    md=self.get_metadata_fn(i)
                    self.tracklist[i]=md['name']
                except: continue
                
        # for test
        if self.simu_mode: 
            self.cash=1000000
            self.holdings={}

    def query(self, query: str) -> str:
        """Get the most match ICodes of assets for the query as well as their descriptions and distances to the query, the closer the better match.

        Args:
            query: The query sentence to retrieve the most match ICodes of assets, for example, Apple company's stock, people's opinions on the president, etc.
        """
        n_results=3
        res=self.icb.query(query,n_results)
        ret=f'Top {n_results} candidates for the query "{query}":\n\n'
        for i in range(n_results):
            ret+=f'{i+1}. Code: {res["ids"][0][i]} (distance {res["distances"][0][i]}) \n\n{res["documents"][0][i]}\n\n'
        return ret

    def query_icode(self,query,n_results=5): # return top n codes
        res=self.icb.query(query,n_results)
        return res['ids'][0]
    
    def buy(self, icode: str, amount: int) -> str:
        """Buy the asset designated by the code with the amount.
        
        Args:
            icode: The ICode of the asset to buy
            amount: The amount of cash used to buy the asset, the amount must be positive, and the amount should be less than the excess cash in the account.
        """
        print('buy',icode,amount)
        if amount<0: return 'ERROR: Amount should be positive'
        if self.simu_mode: 
            self.cash-=amount*1.01
            if icode not in self.holdings: self.holdings[icode]=0
            self.holdings[icode]+=amount
            return 'Succeed.'
        return self.trade_fn(icode,amount) # str

    def sell(self, icode: str, amount: int) -> str:
        """Sell the asset designated by the ICode with the amount.

        Args:
            icode: The ICode of the asset to buy
            amount: The value of the asset you wish to sell, the amount must be positive, and the amount should be less than the value of the asset you own.
        """
        print('sell',icode,amount)
        if amount<0: return 'ERROR: Amount should be positive'
        if self.simu_mode: 
            self.cash+=amount*0.99
            if icode not in self.holdings: return 'ERROR: You cannot sell an asset you do not own.'
            self.holdings[icode]+=amount
            return 'Succeed.'
        return self.trade_fn(icode,-amount) # str
    
    def trade(self, instructions: str) -> str:
        """Trade the assets based on the instructions.

        Args:
            instructions: A series of tradings instructions, each line is one instruction, there are two kinds of instructions with format "BUY [ICode] [amount]", and "SELL [ICode] [amount]", for example: BUY FIN:AAPL 2000\nSELL FIN:GOOG 1000\n
        """
        todo=[]
        for instruction in instructions.split('\n'):
            if instruction=='': continue
            try:
                action,icode,amount=instruction.split(' ')
                amount=int(amount)
            except: return f'ERROR: Instruction {instruction} not valid. Format should be "BUY [ICode] [amount]" or "SELL [ICode] [amount]". The instructions are not executed.'
            if icode not in self.icodelist: return f'ERROR: Code {icode} not found in the ICodebook. Instruction {instruction} not valid. The instructions are not executed.'
            if amount<0: return f'ERROR: Amount should be positive. Instruction {instruction} not valid. The instructions are not executed.'
            if action.upper() not in ['BUY','SELL']: return f'ERROR: Action {action} not found. Instruction {instruction} not valid. The instructions are not executed.'
            todo.append((action,icode,amount))
        succeed=[]
        for action,icode,amount in todo:
            if action.upper()=='BUY': ret=self.buy(icode,amount)
            elif action.upper()=='SELL': ret=self.sell(icode,amount)
            if 'ERROR' in ret: return f'ERROR: Instruction {action} {icode} {amount} not executed due to error: {ret}. The following instructions are executed successfully:\n'+'\n'.join(succeed)
            succeed.append(f'{action} {icode} {amount}')
        return "Succeed. All instructions are executed."

    def wait(self, memo='') -> str: # TODO: handle memo
        """Decide do nothing for now. Wait for the next analysis from the analyst agent.
        
        Args:
            memo: Optional, take a note of key information, clues, hints event to track, or thoughts that may helpful for your furture decision making.
        """
        return 'wait'
    
    def state_message(self) -> str:
        if self.simu_mode:
            state=f"Cash: {self.cash}, holdings: {self.holdings}" #TODO: replace to get_state() after test
            return f'Current status of the account:\n{state}\n'
        return f'Current status of the account:\n{self.state_fn()}\n'
    
    def __call__(self,analysis,time,data=None):
        raise NotImplementedError


class ChatActuator(BaseActuator):
    def __init__(self,
            root,
            trade_fn,
            state_fn,
            probe_fn,
            get_metadata_fn,
            openai_apikey,
            verbose=False,
            model_name="gpt-3.5-turbo-16k",
            temperature=0.2,
            top_p=0.1,
            request_timout=120,
            limit=5,
            simu_mode=False,
            ruleset=[],
        ):
        super().__init__(root,trade_fn,state_fn,probe_fn,get_metadata_fn,
                         openai_apikey,verbose,simu_mode,ruleset)
        self.model_name=model_name
        self.icodelist=get_icodes(root)
        self.limit=limit
        self.temprature=temperature
        self.top_p=top_p

    def get_state(self): return self.state_fn()

    def message(self,role,content,name=None):
        m={'role':role, 'content':content}
        if name is not None: m['name']=name
        if self.verbose: self.show_message(m)
        return m
    
    def show_message(self,message):
        role=message["role"]
        if role=='assistant': role='actuator'
        print(f'[{role}]:\n\n{message["content"]}\n\n')

    def render_context(self,analysis,time):
        messages=[self.message('system',BASE_PROMPT.context+PROMPT.context)]
        if 'nodt' in self.ruleset: messages.append(self.message('system',PROMPT.rule_nodt))
        if 'track' in self.ruleset: messages.append(self.message('system',PROMPT.rule_track.format(tracklist=self.tracklist)))
        messages.append(self.message('system',f'Current time is {time}.'))
        messages.append(self.message('user',
            f'This is the analysis of the latest news, article, etc. from the analyst:\n\n{analysis}\n\n{self.state_message()}\n\nNow, do your judgments, make decisions, and do function callings. Remember to include the reasoning steps in your judgments and decision making process.'))
        return messages
    
    def act(self,action) -> str:
        name=action['name']
        try:
            args=json.loads(action['arguments'])
        except: return f'ERROR: Arguments {action["arguments"]} is not a json.'
        if name=='trade': ret = self.trade(args['instructions'])
        elif name=='wait': 
            if 'memo' not in args: ret=self.wait()
            else: ret=self.wait(args['memo'])
        elif name=='query': ret=self.query(args['query'])
        elif name=='probe': ret=self.probe_fn(args['icode'])
        elif name=='get_metadata': ret=self.get_metadata_fn(args['icode'])
        else: ret=f'ERROR: Function {name} not exist.'
        act_message=self.render_act_message(action,ret)
        return name,act_message

    def render_act_message(self, action, ret) -> str:
        name=action['name']
        args=json.loads(action['arguments'])
        if 'ERROR' in ret: return self.message('system',f'You made an error when calling {name}, try to fix it: {ret}')
        if name=='trade': 
            if 'Succeed' in ret:
                message=f'{ret}\nAfter executing the instruction, the current status of the account is:\n{self.state_message()}\n'
                message+='Now you can stop and wait for more news and opportunities, or you can continue to give more buy or sell instructions based on the analysis, current status of the account, and your action history. If you decide to wait, you can take a note of key information, clues, hints, event to track, or thoughts that may helpful for your furture decision making in the memo.'
            else:
                message=f'There is error in the instructions, the instructions are not executed: {ret}\nTry to fix the error and give the correct instructions again.'
                return self.message('system',message)
        elif name=='query':
            message=f'Here are some candidates for "{args["query"]}":\n{ret}\n'
            message+='You may choose to buy an asset from them or sell an asset you own based on the query result, current status of the account, and the analysis, or do another query, or wait. If you decide to wait, you can take a note of key information, clues, hints event to track, or thoughts that may helpful for your furture decision making in the memo.'
        elif name=='probe':
            message=f'Here is the historical time series and related information for {args["icode"]}:\n{ret}\n'
            message+='You may choose to buy an asset from them or sell an asset you own based on the query result, current status of the account, and the analysis, or do another query, or wait. If you decide to wait, you can take a note of key information, clues, hints event to track, or thoughts that may helpful for your furture decision making in the memo.'
        elif name=='wait':
            message='You decide to wait for the next analysis from the analyst agent.\n'
            if 'memo' in args: message+=f'Your note has been successfully taken in the memo: {args["memo"]}\n'
        elif name=='get_metadata':
            message=f'Here is the metadata for {args["icode"]}:\n{ret}\n'
        return {
            "role": "function",
            "name": name,
            "content": message,
        }


    def __call__(self, analysis, time, data=None):
        action=''
        step=0
        messages=self.render_context(analysis,time)
        while action!='wait' and step<self.limit:
            # messages.append(self.message('system','Now, do your next judgments, make decisions, and do function callings. Remember to include the reasoning steps in your judgments and decision making process.'))
            step+=1
            response = openai.ChatCompletion.create(
                model=self.model_name,
                messages=messages,
                functions=PROMPT.functions,
                function_call="auto",  # auto is default, but we'll be explicit
                temprature=self.temprature,
                top_p=self.top_p
            )
            message = response["choices"][0]["message"]
            if 'function_call' in message:
                action, act_message=self.act(message["function_call"])
            else: 
                action=''
                act_message=self.message('system',f'You did not call any function, you must call one function in your reply. Try again.')
            # messages.append(message)
            messages.append(act_message)
            # if self.verbose: self.show_message(message)
            if self.verbose: self.show_message(act_message)
            second_response = openai.ChatCompletion.create(
                model=self.model_name,
                messages=messages,
                temprature=self.temprature,
                top_p=self.top_p
            )  # get a new response from GPT where it can see the function response
            messages.append(second_response["choices"][0]["message"])
            if self.verbose: self.show_message(second_response["choices"][0]["message"])
        return messages
        
        





































