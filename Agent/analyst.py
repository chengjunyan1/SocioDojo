import random
import openai
import os
import json,copy

from langchain.agents import load_tools
from langchain.agents import initialize_agent
from langchain.agents import AgentType
from langchain.llms import OpenAI

# try:
from Agent.utils import load_json,pexist,pjoin,makedirs,parse_md,get_tracklist,google
import Agent.prompts.base_instruct as BASE_PROMPT
import Agent.prompts.analyst_instruct as PROMPT
# except: 
#     from utils import load_json,pexist,pjoin,makedirs,parse_md
#     import prompts.base_instruct as BASE_PROMPT
#     import prompts.analyst_instruct as PROMPT



class BaseAnalyst:
    """
    Listen to the infostream and make analysis
    Call the assistant to assit the decision making then pass the analysis to actuators
    Args:
        actuator_fn: __call__(x) of an actuator, just pass the analysis, and optional json data
        assistant_fn: __call__(x) of a assistant, just pass the query, and optional json data
        listen_fn: bindpoint of listen, actively config channels to listen
        watch_fn: now can be implemented by SYS query, actively watch the time series or sources
    """
    def __init__(self,actuator,assistant,openai_apikey,verbose=False,
                 listen_fn=None,watch_fn=None,ruleset=[]):
        self.listen_fn=listen_fn
        self.watch_fn=watch_fn
        self.actuator=actuator
        self.assistant=assistant
        self.verbose=verbose
        self.ruleset=ruleset
        os.environ["OPENAI_API_KEY"] = openai_apikey
        openai.api_key=openai_apikey
        if 'track' in self.ruleset:
            self.tracklist=self.actuator.tracklist

    def message(self,role,content,name=None):
        m={'role':role, 'content':content}
        if name is not None: m['name']=name
        if self.verbose: self.show_message(m)
        return m
    
    def render_context(self):
        return [self.message('system',BASE_PROMPT.context+PROMPT.context)]
    
    def actuate(self,analysis,time):
        return self.actuator(analysis,time)

    def ask(self,query,time):
        content,messages=self.assistant(query,time)
        return content,messages
    
    def get_state(self):
        return self.actuator.get_state()
    
    def analyse(self,news,metadata,time):
        raise NotImplementedError
    
    def send(self):
        return 'send'

    def __call__(self,info,data=None): # sequential process news
        self.record={'analyst':[],'assistant':[],'actuator':[]}
        time=str(info["time"])
        for n in info['news']:
            news=f'{n["datetime"]} {n["source"]}:\n\n{n["message"]}\n\n\n'
            metadata=parse_md(n['metadata'])
            send,analysis=self.analyse(news,metadata,time)
            if send: 
                actuator_messages=self.actuate(analysis,time)
                self.record['actuator'].append(actuator_messages)
        return self.record


class ChatAnalyst(BaseAnalyst):
    """
    Passively accept news and make analysis, do not actively watch sources
    """
    def __init__(
            self,
            actuator,
            assistant,
            openai_apikey,
            verbose=False,
            model_name="gpt-3.5-turbo-16k",
            analyse_fn='hnp',
            limit=5,
            debug_mode=False,
            ruleset=[],
            second_response=False,
            serp_apikey=None,
            temprature=0.2,
            top_p=0.1,
        ):
        super().__init__(actuator,assistant,openai_apikey,verbose,ruleset=ruleset)
        self.model_name=model_name
        self.limit=limit
        self.debug_mode=debug_mode
        self.temprature=temprature
        self.top_p=top_p

        self.analyse_option=analyse_fn
        self.second_response=second_response
        self.serp_apikey=serp_apikey

    def state_message(self) -> str:
        return self.get_state()

    def show_message(self,message):
        role=message["role"]
        if role=='assistant': role='analyst'
        print(f'[{role}]:\n\n{message["content"]}\n\n')
        

    def whether_send(self,messages):
        messages=copy.deepcopy(messages)
        messages.append(self.message('system',PROMPT.whether_send))
        response = openai.ChatCompletion.create(
            model=self.model_name,
            messages=messages,
            functions=PROMPT.send_functions,
            function_call="auto",  # auto is default, but we'll be explicit 
            temprature=self.temprature,
            top_p=self.top_p
        )
        message = response["choices"][0]["message"]
        send="function_call" in message and message["function_call"]['name'].lower()=='send'
        if send: messages.append(self.message('system','You decide to send the analysis.'))
        else: messages.append(self.message('system','You decide not to send the analysis.'))
        return send

    def takenote(self,note):
        return self.message('function',f'Your note have been taken into the notebook: {note}','takenote')
    
    def take_note(self,messages):
        messages.append(self.message('system',PROMPT.take_note))
        response = openai.ChatCompletion.create(
            model=self.model_name,
            messages=messages,
            functions=PROMPT.note_functions,
            function_call="auto",  # auto is default, but we'll be explicit 
            temprature=self.temprature,
            top_p=self.top_p
        )
        message = response["choices"][0]["message"]
        if "function_call" in message and message["function_call"]['name'].lower()=='takenote': 
            try:
                args=json.loads(message["function_call"]['arguments'])
                self.takenote(args['note'])
            except: messages.append(self.message('system',f'ERROR: Arguments {message["function_call"]["arguments"]} is not a json. You must provide a json as arguments.'))
    
    def whether_read(self,metadata,messages):
        messages=copy.deepcopy(messages)
        read=False
        if metadata=="": read=True
        else:
            messages.append(self.message('system',PROMPT.whether_read.format(metadata=metadata)))
            response = openai.ChatCompletion.create(
                model='gpt-3.5-turbo',
                messages=messages,
                functions=PROMPT.read_functions,
                function_call="auto",  # auto is default, but we'll be explicit 
                temprature=self.temprature,
                top_p=self.top_p
            )
            message = response["choices"][0]["message"]
            if "function_call" in message and message["function_call"]['name'].lower()=='read': 
                act_message=self.message('system',"You decide to read the full content.")
                read=True
            else: act_message = self.message('system',"You decide not to read the full content.")
            messages.append(act_message)
        return read

    def basic_analysis(self,news,messages,time,**kwargs):
        messages.append(self.message('user',PROMPT.analysis_template.format(time=time,news=news,account_status=self.state_message())))
        response = openai.ChatCompletion.create(
            model=self.model_name,
            messages=messages,
            temprature=self.temprature,
            top_p=self.top_p
        )
        message = response["choices"][0]["message"]
        content=message["content"]
        if self.verbose: self.show_message(message)
        messages.append(message)
        return messages,content
    
    def handle_call(self,message,messages,time):
        done=False
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
                    message=self.message('system','You are prepared to give the final analysis report.')
                elif fn=='ask': 
                    sys_message=self.message('system',f'You ask the assistant to help you with the analysis: {args["query"]}')
                    messages.append(sys_message)
                    ret,assistant_messages=self.ask(args['query'],time)
                    message=self.message('function',ret,fn)
                    self.record['assistant'].append(assistant_messages)
                # elif fn=='continue':
                #     message=self.message('system','You choose continue the analysis.')
                else: 
                    message=self.message('system',f'Invalid function call: {fn} Valid function calls are ask and done.')
        else: 
            message=self.message('system','You do not call any function. You must call one of ask or done.')
        messages.append(message)
        return done,messages
    
    def do_analysis(self,news,messages,time,limit=5):
        messages.append(self.message('user',PROMPT.analysis_template.format(time=time,news=news,account_status=self.state_message())))
        if self.analyse_option=='hnp':
            messages.append(self.message('system',PROMPT.hypothesis_proof))
        elif self.analyse_option=='pns':
            messages.append(self.message('system',PROMPT.pns_prompt))
        while limit>0:
            limit-=1
            fn=PROMPT.hnp_functions
            response = openai.ChatCompletion.create(
                model=self.model_name,
                messages=messages,
                functions=fn,
                function_call="auto", 
                temprature=self.temprature,
                top_p=self.top_p
            )
            message = response["choices"][0]["message"]
            done,messages=self.handle_call(message,messages,time)
            if done: break
            if self.second_response:
                second_response = openai.ChatCompletion.create(
                    model=self.model_name,
                    messages=messages,
                    temprature=self.temprature,
                    top_p=self.top_p
                ) 
                message=second_response["choices"][0]["message"]
                messages.append(message)
                if self.verbose: self.show_message(message)
            # if done and self.second_response:
            #     return messages,message["content"]
        fr_prompt=PROMPT.final_report_hnp if self.analyse_option=='hnp' else PROMPT.final_report
        messages.append(self.message('system',fr_prompt))
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
    
    def analyse(self,news,metadata,time): # news: string, metadata: string (parsed)
        messages=self.render_context()
        messages.append(self.message('system',f'Current time is {time}.'))
        if 'track' in self.ruleset: 
            messages.append(self.message('system',PROMPT.rule_track.format(tracklist=self.tracklist)))
        if self.debug_mode: read=True
        else: read=self.whether_read(metadata,messages)
        if read:
            messages,content=self.do_analysis(news,messages,time,limit=self.limit)
            if self.debug_mode: send=True
            else: send=self.whether_send(messages) 
            # self.take_note(messages)
            self.record['analyst'].append(messages)
            self.record['read']=True
            self.record['send']=send
            return send,content
        else: 
            self.record['analyst'].append(messages)
            self.record['read']=False
            self.record['send']=False
            return False,None
        


