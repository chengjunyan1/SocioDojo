import json
import sys,os
import functools as ft
import pandas as pd
from datetime import timedelta,datetime
import pytz
# sys.path.append(".")  # Add the parent directory to sys.path


from Env.const import ERRREP,ERREXC,ERRTRA,COMMISSION,OVERNIGHT,BOUND
from Env.utils import str2utc,utc2str,get_tracklist,get_overnight



"""
Task module build upon HWM
Decision & Execution
"""



class Asset:
    """
    q: query code in OneProbe
    d: buying date
    Once buy or query an asset, a cache is created (span fixed), so no cost when update
    """
    def __init__(self,symbol,value,price,name): 
        self.symbol=symbol
        # self.short=short # for short, 30% gain is 30% loss
        self.price=float(price)
        self.value=float(value) # amount
        self.name=name

        self.unrealize=0
        self.base=float(value)
        self.size=self.value/self.price
        self.overnight_total=0


    def update(self,new_price): # since there are cache, so update is no cost
        new_price=float(new_price)
        gain=(new_price-self.price)/self.price
        # if self.short:
        #     self.value=self.value*(1-gain)
        # else: 
        self.value=self.value*(1+gain)
        self.unrealize=self.value-self.base
        self.price=new_price
    
    def trade(self,amount): # amount can lower than 0, not check here 
        amount=float(amount)
        realized=0
        if amount<0:
            ratio=(self.value+amount)/self.value # residual
            realized=self.unrealize*(1-ratio)
            self.base*=ratio
            self.unrealize*=ratio
        else:
            self.base+=amount
        self.value+=amount
        return -amount,realized # cash return, if it is sell, its positive
    
    def tojson(self,total=None):
        jsf = {
            'symbol': self.symbol,
            'name': self.name,
            'value':round(self.value,1),
            'price': self.price,
            # 'short': self.short,
            'unrealize': round(self.unrealize,1),
            'base': round(self.base,1),
            'size': round(self.size,6),
            'overnight_total': self.overnight_total
        }
        if total is not None:
            jsf['ratio']=round(self.value/total,4)
        return jsf

def json2asset(json):
    asset=Asset(json['symbol'],json['value'],json['price'],json['name'])#,json['short'])
    asset.base=json['base']
    asset.unrealize=json['unrealize']
    asset.size=json['size']
    asset.overnight_total=json['overnight_total']
    return asset



class HyperPortfolio:
    def __init__(self,name,root,init_time,oneprobe,ruleset):#,history_span='2018-01-01 2023-07-31',cache=True,interpolate=True):
        self.name=name
        self.root=root
        # self.op=OneProbe(root,history_span,cache=cache,interpolate=interpolate)
        self.op=oneprobe
        self.ruleset=ruleset
        self.balance=0 # have to clear balance before trade

        self.time= pd.to_datetime(init_time, utc=True) # date str or UTC datetime
        self.assets={} # indexed by symbol
        self.transactions=[]
        self.records=[]

    def get_total(self):
        total=0
        for code in self.assets:
            total+=self.assets[code].value
        return total
    
    def get_price(self,q,check=True):
        prior=self.op.probe(q,self.time,check=check) 
        if 'ERROR' in prior: return prior
        if len(prior)==0: return 'ERROR: no data'
        price=prior.tail(1).iloc[0]['value']
        return price
    
    def update_price(self,overnight=False):
        close=[]
        for code in self.assets:
            q=code
            price=self.get_price(q,check=False)
            if isinstance(price,str) and 'ERROR' in price: 
                continue # only no data is possible, maybe no update within 12 months
            self.assets[code].update(price)
            if overnight:
                domain=q.split(':')[0]
                value=self.assets[code].size*OVERNIGHT[domain]*price/360
                self.assets[code].overnight_total+=value
                self.balance+=value
            value=self.assets[code].value
            base=self.assets[code].base
            overnight=self.assets[code].overnight_total
            gnl=value-overnight-base
            if gnl<-BOUND*base: 
                close.append((code,base))
                self.balance-=(overnight+base-BOUND*base)
            if  gnl>BOUND*base:
                close.append((code,base))
                self.balance-=(overnight+base+BOUND*base)
        for i,base in close: 
            del self.assets[i]
            self.records.append((f'{self.time}', 'Close',f'{i}, Base: {round(base,1)}'))

    def movetime(self,days=0,hours=0,minutes=0,seconds=0):
        newtime=self.time+timedelta(days=days,hours=hours,minutes=minutes,seconds=seconds)
        overnight=get_overnight(self.time,newtime)
        for time in overnight:
            self.time=time
            self.update_price(overnight=True)
        self.time=newtime
        self.update_price()
    
    def settime(self,newtime): 
        overnight=get_overnight(self.time,newtime)
        for time in overnight:
            self.time=time
            self.update_price(overnight=True)
        self.time=newtime
        self.update_price()

    def checkdt(self,q):
        lastbuy=datetime.min.replace(tzinfo=pytz.timezone("UTC"))
        for transaction in self.transactions[::-1]:
            code,amount,dt,price,realized,name=transaction
            if code==q and amount>0:
                if lastbuy<str2utc(dt): 
                    lastbuy=str2utc(dt)
        if (self.time-lastbuy).days<5: return True
        return False
    
    def trade(self,q,amount):#,short=False):
        amount=float(amount)
        if amount==0: return ERRTRA+' amount is 0'
        # code=q+':S' if short else q+':L' # asset code
        code=q
        realized=0
        if code in self.assets: # only invest amount change, same time price same
            if self.assets[code].value+amount<0: return ERRTRA+' not enough asset to sell'
            if amount<0 and 'nodt' in self.ruleset: # sell and nodt
                if self.checkdt(code): return ERRTRA+' you are not allowed to sell an asset within 5 days of buying it.'
            change,realized=self.assets[code].trade(amount)
            if self.assets[code].value+amount==0:
                del self.assets[code]
            name=self.assets[code].name
        else: # buy or sell new asset
            if amount<0: # not allow naked sell now 
                return ERRTRA+' do not allow naked sell an asset you do not own'
            price=self.get_price(q)
            if isinstance(price,str) and 'ERROR' in price: return price
            md=self.op.get_metadata(q)
            name=md['name']
            self.assets[code]=Asset(q,amount,price,name)#,short)
            change=-amount
        price=self.assets[code].price
        self.transactions.append((code,amount,utc2str(self.time),price,realized,name))
        return change

    def holdings(self,total_asset=None):
        holdings={}
        holdings['total']=self.get_total()
        for i in self.assets:
            holdings[i]=self.assets[i].tojson(total_asset)
        holdings['recent transactions']=[]
        for i in self.transactions[-10:]:
            code,amount,dt,price,realized,name=i
            holdings['recent transactions'].append({
                'code':code,
                'name': name,
                'amount':round(amount,1),
                'datetime':dt,
                'price':round(price,4),
                'realized':round(realized,1),
            })
        return holdings

    def json(self,total_asset=None):
        assets={}
        for code in self.assets:
            assets[code]=self.assets[code].tojson(total_asset)
        return {
            'name':self.name,
            'time':utc2str(self.time),
            'assets':assets,
            'transactions':self.transactions,
            'records':self.records,
            'ruleset':self.ruleset,
            'balance':self.balance,
        }


def test_hp(root):
    print('|----------Test Hyperportfolio Module----------|')
    from Env.interfaces.probe import OneProbe
    history_span='2018-01-01 2023-07-31'
    op=OneProbe(root,history_span)
    hp=HyperPortfolio("test",root,"2022-01-01",op)
    q='YGV:FOEC.NS.B'
    # prior,res=hp.query(q)

    hp.trade(q,100)
    hp.movetime(days=1)
    hp.trade(q,-50)
    # hp.save()

    print(hp.json())

    # print(prior,res)

    print('|----------Hyperportfolio Module Pass----------|')
    print()



class Account:
    def __init__(self,name,root,oneprobe,ruleset,init_cash=1e6):
        self.name=name
        self.hps={}
        self.create_hp=ft.partial(HyperPortfolio,root=root,oneprobe=oneprobe,ruleset=ruleset)
        self.cash=init_cash 
        self.root=root
    
    def new_portfolio(self,name,time):
        if name in self.hps: return ERRREP+' hyper portfolio name already used'
        self.hps[name]=self.create_hp(name,init_time=time)
        return 'Succeed.'
    
    def trade(self,name_hp,q,amount):#,short=False):
        commission=abs(amount)*COMMISSION
        fare=amount+commission
        if amount>0 and self.cash<fare: return ERREXC+' not enough cash'
        cash_change=self.hps[name_hp].trade(q,amount)#,short)
        if isinstance(cash_change,str) and 'ERROR' in cash_change:
            return ERREXC+' failed to trade this instrument: '+cash_change
        self.cash+=cash_change-commission
        return 'Succeed.'
    
    def movetime(self,days=0,hours=0,minutes=0,seconds=0):
        for i in self.hps: 
            self.hps[i].movetime(days,hours,minutes,seconds)
            self.cash-=self.hps[i].balance
            if self.hps[i].balance!=0:
                self.hps[i].records.append((f'{self.hps[i].time}', 'Balance',f'{i}, change: {round(self.hps[i].balance,1)}'))
                self.hps[i].balance=0

    def settime(self,time):
        for i in self.hps: 
            self.hps[i].settime(time)
            self.cash-=self.hps[i].balance
            if self.hps[i].balance!=0:
                self.hps[i].records.append((f'{self.hps[i].time}', 'Balance',f'{i}, change: {round(self.hps[i].balance,1)}'))
                self.hps[i].balance=0
    
    def get_total(self):
        total=0
        for i in self.hps:
            total+=self.hps[i].get_total()
        return total

    def assets_report(self,detailed=False):
        portfolios={}
        total=self.get_total()
        total_asset=self.cash+total
        for i in self.hps:
            if detailed: portfolios[i]=self.hps[i].json(total_asset)
            else: portfolios[i]=self.hps[i].holdings(total_asset)
        return {
            'account name':self.name,
            'cash':round(self.cash,1),
            'cash ratio':round(self.cash/total_asset,4),
            'assets value': round(total,1),
            'assets ratio': round(total/total_asset,4),
            'total value': round(total_asset,1),
            'portfolios': portfolios
        }

    def json(self):
        hps={}
        total=self.get_total()
        total_asset=self.cash+total
        for hp in self.hps:
            hps[hp]=self.hps[hp].json()
        return {
            'name':self.name,
            'total value': round(total_asset,1),
            'protfolios': hps,
            'cash': self.cash,
            'cash ratio':round(self.cash/total_asset,4),
            'assets value': round(total,1),
            'assets ratio': round(total/total_asset,4),
        }
    
    def save(self,savename):
        ckpt=self.json()
        path=os.path.join(self.root,'Ckpts',savename,'Account')
        if not os.path.exists(path): os.makedirs(path)
        with open(os.path.join(path,self.name+'.json'),'w') as f: json.dump(ckpt,f)

    def load(self,savename):
        path=os.path.join(self.root,'Ckpts',savename,'Account',self.name+'.json')
        with open(path,'r') as f: ckpt=json.load(f)
        hps=ckpt['protfolios']
        self.cash=ckpt['cash']
        for i in hps:
            hp=hps[i]
            self.hps[i]=self.create_hp(i,init_time=str2utc(hp['time']))
            self.hps[i].transactions=hp['transactions']
            self.hps[i].records=hp['records']
            self.hps[i].balance=hp['balance']
            self.hps[i].ruleset=hp['ruleset']
            for code in hp['assets']:
                self.hps[i].assets[code]=json2asset(hp['assets'][code])



class Broker:
    def __init__(self,root,savename,init_time,uniquery,oneprobe,ruleset):
        self.uq=uniquery
        self.op=oneprobe
        self.create_account=ft.partial(Account,root=root,oneprobe=oneprobe,ruleset=ruleset)
        self.accounts={} # accounts
        self.time=pd.to_datetime(init_time, utc=True)
        self.root=root
        self.savename=savename
        self.ruleset=ruleset
        if 'track' in self.ruleset:
            tracklist=get_tracklist(root)
            self.tracklist={}
            for i in tracklist:
                try:
                    md=self.op.get_metadata(i)
                    self.tracklist[i]=md['name']
                except: continue
                
    def register(self,name):
        if name in self.accounts: return ERRREP+' account name already exist'
        self.accounts[name]=self.create_account(name)
        self.accounts[name].new_portfolio('default',self.time)

    def query(self,q,info=False): # query an asset now, HEAD:QUERY, this gives information about the ticker
        prior=self.op.probe(q,self.time) 
        if 'ERROR' in prior: return prior
        price=prior.tail(1).iloc[0]['value']
        ret={'current':price,'history':prior}
        if info: 
            res=self.uq.query('PRB:'+q,self.time) # PRB:QUERY, provide information
            ret['information']=res
        return ret

    def movetime(self,days=0,hours=0,minutes=0,seconds=0):
        self.time+=timedelta(days=days,hours=hours,minutes=minutes,seconds=seconds)
        for i in self.accounts: self.accounts[i].movetime(days,hours,minutes,seconds)

    def settime(self,time):
        self.time=time
        for i in self.accounts: self.accounts[i].settime(time)

    def trade(self,account,q,amount,hp='default'):
        if 'track' in self.ruleset:
            if q not in self.tracklist: 
                return {'result':ERRTRA+f' ICode {q} not in tracklist'}
        ret=self.accounts[account].trade(hp,q,amount)
        return {'result':ret}
                                                                                                                                                                                                                                                
    def new_portfolio(self,account,name_hp):
        self.accounts[account].new_portfolio(name_hp,self.time)
    
    def account_report(self,account,detailed=False):
        return self.accounts[account].assets_report(detailed)
    
    def report(self,detailed=True,account_detail=False):
        info={
            'accounts':list(self.accounts.keys()),
            'time':utc2str(self.time)
        }
        if detailed:
            accounts={}
            for i in self.accounts:
                accounts[i]=self.account_report(i,account_detail)
            info['accounts']=accounts
        return info
    
    def save(self):
        path=os.path.join(self.root,'Ckpts',self.savename)
        ckpt=self.report(detailed=False)
        ckpt['name']=self.savename
        if not os.path.exists(path): os.makedirs(path)
        with open(os.path.join(path,'broker.json'),'w') as f: json.dump(ckpt,f)
        for i in self.accounts:
            self.accounts[i].save(self.savename)

    def load(self):
        path=os.path.join(self.root,'Ckpts',self.savename,'broker.json')
        if not os.path.exists(path): return False
        with open(path,'r') as f: ckpt=json.load(f)
        self.time=str2utc(ckpt['time'])
        for name in ckpt['accounts']:
            self.accounts[name]=self.create_account(name)
            self.accounts[name].load(self.savename)
        return True


def test_broker(root,history_span,apikeys):
    print('|----------Test Broker Module----------|')
    from Env.interfaces.query import UniQuery
    from Env.interfaces.probe import OneProbe
    uq=UniQuery(root,apikeys)
    op=OneProbe(root,history_span)
    init_date="2021-10-01"
    savename='test001'
    account='default'
    broker=Broker(root,savename,init_date,uq,op)
    broker.register(account)
    q='PRC:AAPL'
    info=broker.query(q)
    broker.trade(account,q,1000)
    broker.trade(account,'WEB:Computer Science',1000)#,short=True)
    broker.movetime(days=1)
    print('----------Before Save----------')
    print(broker.report())
    broker.save()
    broker=Broker(root,savename,init_date,uq,op)
    print('----------Create New----------')
    print(broker.report())
    print('----------After Load----------')
    broker.load()
    print(broker.report())


    print('|----------Broker Module Pass----------|')
    print()

