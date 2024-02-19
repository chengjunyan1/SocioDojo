import functools as ft
import pandas as pd

from Env.interfaces.watch import InfoStream
from Env.const import ERRFAIL,ERREMP
from Env.utils import utc2str



class WatchList(InfoStream):
    """
    Summary of the watched assets
    """
    def __init__(self, init_time, op, symbols=[],levels=0.05): # level: level of movement to be detected as a message
        self.probe=op.probe
        self.symbols=symbols
        assert isinstance(levels,float) or isinstance(levels,dict)
        if isinstance(levels,float): 
            self.levels={}
            for i in self.symbols: self.levels[i]=levels
        else: self.levels=levels # level of movements that mcause a message
        super().__init__(None, init_time)


    def load_data(self,tend='2023-07-31'):
        if len(self.symbols)==0: return -1
        t=pd.to_datetime(tend, utc=True)
        self.tss={}
        indexes=[]
        self.metadata={} # reset metadata
        self.messages={} # reset messages
        for s in self.symbols:
            if s in self.levels: level=self.levels[s]
            elif s.split[':'][0] in self.levels: level=self.levels[s.split[':'][0]]
            elif 'default' in self.levels: level=self.levels['default'] 
            else: raise f'Level dict incorrect, symbol {s} not found in dict with either symbol or head'
            ts=self.probe(s,t,False)
            self.tss[s]=ts
            # detect moves by simply one time move
            last=None
            last_t=None
            for index, row in enumerate(ts.iterrows(), start=1):
                dt, row_data = row
                cur=row_data['value']
                if last is None: 
                    last=cur
                    last_t=dt
                    continue
                diff=(last-cur)/last
                if abs(diff)>=level: # detect movement
                    ind=s+'-'+str(index)
                    indi={}
                    indi['datetime']=dt
                    indi['path']=ind
                    indexes.append(indi)
                    self.metadata[ind]={}
                    self.messages[ind]={'Symbol':s,'Move':f'{diff*100:.4f}%','Current':cur,'Time':utc2str(dt),'Last':last,'Last time':utc2str(last_t)}
                last=cur
                last_t=dt
        df = pd.DataFrame(indexes)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index('datetime', inplace=True)
        df.sort_index(inplace=True)
        self.index=df 
        return len(self.messages)

    def check(self,t): # check watch list of time t
        time = pd.to_datetime(t, utc=True)
        res=[]
        for s in self.tss:
            ts=self.tss[s]
            ts.index = pd.to_datetime(ts.index,utc=True)
            prior = ts[ts.index <= time]
            if len(prior)>0:
                resi={}
                tail=prior.tail(1)
                resi['symbol']=s
                resi['current']=tail['value'].values[-1]
                resi['current_time']=utc2str(tail.index.tolist()[-1])
            if len(prior)>=2:
                tails=prior.tail(2)
                resi['last']=tails['value'].values[-2]
                resi['last_time']=utc2str(tails.index.tolist()[-2])
                resi['move']=(resi['last']-resi['current'])/resi['last']
            res.append(resi)
        return res

    def update_levels(self,levels):
        assert isinstance(levels,float) or isinstance(levels,dict)
        if isinstance(levels,dict):
            for i in levels:
                self.levels[i]=levels[i]
        else: 
            for i in self.levels: self.levels[i]=levels[i]

    def add_symbols(self,symbols,levels=None):
        for symbol in symbols:
            if symbol not in self.symbols:
                self.symbols.append(symbol)
        if levels is not None:
            if isinstance(levels,float): 
                lvs={}
                for i in symbols: lvs[i]=levels
            else: lvs=levels
            self.update_levels(lvs)
        self.load()

    def del_symbols(self,symbols):
        for symbol in symbols:
            if symbol in self.symbols: del self.symbols[symbol]
            if symbol in self.levels: del self.levels[symbol]
        self.load()

    def reload(self): self.index_ptr=-1 # no need to reload


def test_wl(root,apikeys,sysddir):
    from Env.interfaces.probe import OneProbe
    op=OneProbe(root,apikeys,sysddir)
    symbols=['FIN:AAPL','FRD:GDP','FTE:DP']
    wl=WatchList('2022-01-01',op,symbols)

    res=wl.check('2023-01-01')
    print(res)

    wl.top()
    while True:
        ret=wl.pop()
        if ERREMP in ret: 
            print(ret)
            break
        print(ret)




class SummaryFlow(InfoStream):
    """
    Summary of the world/domain
    """
    def __init__(self, path, init_time, reload_freq=None):
        super().__init__(path, init_time, reload_freq)
        pass






