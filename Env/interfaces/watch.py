import os,time
import json
import pandas as pd
from datetime import datetime,timedelta
from dateutil import parser
import pytz

from Env.utils import date_add,first_day_of_next_month,utc2str,pjoin,pexist,makedirs,load_json,save_json
from Env.const import ERREMP


MONTHS=['january','february','march','april','may','june',
'july','august','september','october','november','december']


class InfoStream:
    """
    A queue of the information, one can build customized info source with it
    Information flow can be implemented as either a InfoStream, or directly from agent message
    """
    def __init__(self,path,init_time,reload_freq=None):
        self.path=path
        self.time=pd.to_datetime(init_time, utc=True) # utc time
        self.index_ptr=0
        self.reload_freq=reload_freq
        self.load() # read message of a new period, sort by datetime 

    def load_data(self): # today's messages --> self.index, self.metadata, self.messages
        # self.index: dataframe indexed by datetime, one column of 'path'
        # self.metadata: metadata dict, key is 'path'
        # self.messages: messages dict, key is 'path
        raise NotImplementedError

    def preload_data(self):
        df=pd.read_csv(pjoin(self.path,'index.csv'))
        df.set_index('datetime', inplace=True)
        df.sort_index(inplace=True)
        t0=time.time()
        self.messages=load_json(pjoin(self.path,'messages.json'))
        print(f'Messages loaded, spent {time.time()-t0:.2f} secs, path: {self.path}')
        self.metadata=load_json(pjoin(self.path,'metadata.json'))
        self.index=df
        return len(self.messages)

    def load(self):
        lens=self.load_data()
        if lens==-1: 
            self.index_ptr=-1
            return -1
        while True:
            toptime=self.top()[0]
            if toptime>=self.time: break
            self.pop()
        return lens

    def reload(self): # set time, load new batch, e.g. daily, monthly
        # when queue is empty, reload, return length of queue, when all loaded, return -1
        self.index_ptr=0
        # if self.reload_freq is None: raise NotImplementedError
        if self.reload_freq=='daily': 
            date=utc2str(self.time).split(' ')[0]
            date=date_add(date,1)
            self.time=pd.to_datetime(date, utc=True)
        elif self.reload_freq=='monthly': 
            date=utc2str(self.time).split(' ')[0]
            date=first_day_of_next_month(date) # load monthly
            self.time=pd.to_datetime(date, utc=True)
        self.load()

    def move_ptr(self):
        self.index_ptr+=1
        if self.index_ptr>=len(self.index):
            if self.reload_freq is None: 
                self.index_ptr=-1
                return -1
            self.reload()

    def top(self): # get the information about the top message, return: (time, metadata) or ERREMP
        if self.index_ptr==-1: return ERREMP+' the queue is empty'
        time=self.index.index[self.index_ptr]
        path=self.index.iloc[self.index_ptr]['path']
        metadata=self.metadata[path]
        return pd.to_datetime(time, utc=True),metadata

    def pop(self): # return: (time, message) or ERREMP
        if self.index_ptr==-1: return ERREMP+' the queue is empty'
        time=self.index.index[self.index_ptr]
        path=self.index.iloc[self.index_ptr]['path']
        message={}
        message['metadata']=self.metadata[path]
        message['message']=self.messages[path]
        self.move_ptr()
        return pd.to_datetime(time, utc=True),message



class Channel(InfoStream): # message is a string, metadata is json
    def __init__(self,root,channel,init_time):
        super().__init__(os.path.join(root,'Corpus','IS','Channels',channel),init_time)

    def load_data(self): return self.preload_data()



class AllWatch:
    """
    return: time,source,message
    """
    def __init__(self,root,init_time,channels=['WSJ','NYT'],preload=True):
        init_time=pd.to_datetime(init_time, utc=True) # datestr or standard time
        self.queues={}
        self.channels_book=load_json(os.path.join(root,'Corpus','IS','Channels.json'))
        for i in channels: 
            assert i in self.channels_book, f'Channel {i} not found. You may create a customized channel.'
            self.queues[i]=(i,Channel(root,i,init_time))
        self.root=root
    
    def top(self):
        next_time=[]
        for i in self.queues:
            _,q=self.queues[i]
            ret=q.top() # time,metadata
            if ERREMP in ret: next_time.append(datetime.max.replace(tzinfo=pytz.timezone("UTC")))
            else: 
                time,_=ret
                next_time.append(time)
        min_datetime=min(next_time) # get the earlier one
        ind = next_time.index(min_datetime)
        min_id=list(self.queues.keys())[ind]
        return min_id,min_datetime 
    
    def add_channel(self,channel_id,time,queue=None): # queue: (str,constructed InfoStream)
        if channel_id in self.queues: raise f'Channel {channel_id} already added.'
        if queue is None:
            if channel_id not in self.channels_book: raise f'Channel {channel_id} not found. You may create a customized channel.'
            self.queues[channel_id]=(channel_id,Channel(self.root,channel_id,time))
        else: self.queues[channel_id]=queue

    def del_channel(self,channel_id):
        if channel_id not in self.queues: raise f'Channel {channel_id} not found.'
        del self.queues[channel_id]

    def update_chennels(self,channels,time): # channels: {id:(name: queue) or None}
        todel=[]
        for i in self.queues:
            if i not in channels: todel.append(i)
        for i in todel: self.del_channel(i)
        for i in channels:
            if i not in self.queues: self.add_channel(i,time,channels[i])
    
    def settime(self,time):
        while True:
            ret=self.top()
            if ERREMP in ret: return ERREMP+' no more messages'
            _,min_t=ret
            if min_t>=time: return 'Succeed.'
            self.pop()

    def pop(self): # passive mode
        min_id,_=self.top()
        source,queue=self.queues[min_id]
        ret=queue.top()
        if ERREMP in ret: return ERREMP+' no more messages'
        time,_=ret
        time,message=queue.pop()
        return [{
            'datetime':time,
            'source':source,
            'metadata': message['metadata'],
            'message': message['message']
        }]
    
    def tpop(self,t,span_secs,watch=[]): # pop messages between t and t+span, active/mixed mode, or directly return is watch list update
        messages=[]
        t_next=t+timedelta(seconds=span_secs)
        while True:
            min_id,min_datetime=self.top()
            if min_datetime==datetime.max.replace(tzinfo=pytz.timezone("UTC")):
                return ERREMP+' no more messages'
            if min_datetime>t_next: break
            if min_datetime<t: # skip previous news
                self.pop()
                continue
            ret=self.pop()
            if ERREMP in ret: 
                if messages==[]: return ERREMP+' no more messages'
                else: break
            messages.append(ret)
            if min_id in watch: return messages,min_datetime # if a watching source update, directly return without wait to interrupt
        return messages,t_next # interupt when t_next==t+span_secs
    

def test_aw(root,all=False):
    print('|----------Test All Watch----------|')
    init_date='2021-10-01 12:00'
    # end_date='2021-10-02'
    # init_date='2023-07-31'
    channels=['WSJ','XAD','TTT']
    if all: channels=list(load_json(os.path.join(root,'Corpus','IS','Channels.json')))
    aw=AllWatch(root,init_date,channels) 

    # test the entire queue
    t0=time.time()
    while True:
        ret=aw.pop()
        if ERREMP in ret: 
            print(ret)
            break
        print(ret['datetime'],ret['source'],time.time()-t0)
        # if ret['datetime'].strftime('%Y-%m-%d')==end_date:
        #     break
    print('Time:',time.time()-t0)
        

    print('|----------All Watch Pass----------|')
    print()
    


































