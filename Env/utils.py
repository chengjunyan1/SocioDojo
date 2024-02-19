from datetime import datetime, timedelta
from dateutil import parser
import numpy as np
import pandas as pd
import pytz,json
import os,re

try:
    from .const import ERRINV,Buggy
except:
    from const import ERRINV,Buggy


pjoin=os.path.join
pexist=os.path.exists


def utc2str(time): # time like 2016-11-04 00:00:00+00:00
    return time.strftime("%Y-%m-%d %H:%M:%S%z")

def str2utc(timestr): # reverse above
    return datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S%z")

def date_add(original_date_str,d=0,w=0,y=0):
    original_date = datetime.strptime(original_date_str, "%Y-%m-%d")
    w+=52*y
    result_date = original_date + timedelta(days=d,weeks=w)
    result_date_str = result_date.strftime("%Y-%m-%d")
    return result_date_str  # Output: 2022-02-15

def first_day_of_next_month(input_date_str):
    input_date = datetime.strptime(input_date_str, "%Y-%m-%d")
    next_month = input_date.replace(day=1) + timedelta(days=32)
    first_day_next_month = next_month.replace(day=1)
    return first_day_next_month.strftime("%Y-%m-%d")

def get_dates_between(start_date_str, end_date_str):
    # Convert the input strings to datetime objects
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    # Initialize the list to store the dates
    dates_list = []
    # Generate the dates and add them to the list
    current_date = start_date
    while current_date <= end_date:
        dates_list.append(current_date.date().strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    return dates_list

def interpolate(original_points,num_interpolated_points=6,mode="linear"):
    interpolated_points=[]
    original_points=np.array(original_points)
    for i in range(len(original_points)-1):
        start=original_points[i]
        end=original_points[i+1]
        step_size = (end - start) / (num_interpolated_points + 1)
        points=np.linspace(start + step_size, end - step_size, num=num_interpolated_points)
        if mode=="rand": # good for gt 
            points+=(np.random.randn(num_interpolated_points)-0.5)*original_points.std()
        if mode=='same': # good for yf
            points=np.array([start]*num_interpolated_points)
        interpolated_points.append(start)
        interpolated_points+=points.tolist()
    interpolated_points.append(original_points[-1])
    return interpolated_points

def str2date(datestr): 
    y,m,d=[int(i) for i in datestr.split('-')]
    return datetime(y,m,d)

def str2datetime(date_string):
    # date_string = "2023-08-06 15:30:45"  # Replace this with your desired date and time string
    date_format = "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(date_string, date_format)

def date2str(date): return date.strftime('%Y-%m-%d')

def date_compare(date1,date2):
    return (str2date(date1)-str2date(date2)).days

def interpolate_dates(date,value,mode="linear"):
    days=date_compare(date[1],date[0])-1
    if days==0: return [],[]
    dates=get_dates_between(date[0],date[1])[1:-1]
    values=interpolate(value,days,mode)[1:-1]
    return dates,values

# date=['2021-09-08','2021-09-13']
# value=[202,302]
# interpolate_dates(date,value)

def kw_interpolate(df,kw,mode="linear"):
    date=[]
    value=[]
    dates=df.index
    for i in range(1,len(df)):
        date_pair=date2str(dates[i-1]),date2str(dates[i])
        value_pair=[df.iloc[i-1][kw],df.iloc[i][kw]]
        idates,ivalues=interpolate_dates(date_pair,value_pair,mode)
        date.append(date_pair[0])
        date+=idates
        value.append(value_pair[0])
        value+=ivalues
    date.append(date_pair[1])
    value.append(value_pair[1])
    return date,value

def df_interpolate(df,kw_list,mode="linear"): # assume date as index
    ndf = pd.DataFrame()
    df = df.sort_index()
    if not isinstance(kw_list,list): kw_list=[kw_list]
    for kw in kw_list: 
        values=df[kw].values
        if (values!=0).sum()==0: return ERRINV+' all zero time series'
        mean=values.sum()/(values!=0).sum()
        for i in range(len(values)): 
            if values[i]==0: values[i]=mean+(np.random.rand()-0.5)*values.std()
        date,value=kw_interpolate(df,kw,mode)
        ndf[kw]=value
    ndf['date']=date
    return ndf

def replace_forbidden_chars(file_name):
    file_name=file_name.replace(' ','_')
    forbidden_chars = r'<>:"/\|?*'
    translation_table = str.maketrans(forbidden_chars, '_' * len(forbidden_chars))
    return file_name.translate(translation_table).lower()

def translate_forbidden_chars(file_name):
    file_name = file_name.replace(' ', '_')
    forbidden_chars = r'<>:"/\|?*'
    translation_dict = {char: str(index) for index, char in enumerate(forbidden_chars)}
    for char, index in translation_dict.items():
        file_name = file_name.replace(char, index)
    return file_name


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

def get_icodes(root):
    with open(pjoin(root,'Corpus','TS','icode_list.txt'),'r',encoding='utf-8') as f:
        icodes=[i.strip() for i in f.readlines()]
        icodes=[i for i in icodes if i not in Buggy]
        return icodes

def get_tracklist(root):
    with open(pjoin(root,'Corpus','TS','tracklist.txt'),'r',encoding='utf-8') as f:
        return [i.strip() for i in f.readlines()]
    
def detect_datetime_or_timestamp(s):
    # Try to identify if it is a timestamp (only contains digits)
    if re.fullmatch(r'\d+', s):
        return "timestamp"
    else:
        # Try to parse it as a date/time string
        try:
            parser.parse(s)
            return "date/time"
        except:
            return "unknown"
        
def get_overnight(start_time,end_time):
    date_range = pd.date_range(start_time.date(), end_time.date() + pd.Timedelta(days=1))
    date_range=[pd.to_datetime(i,utc=True) for i in date_range]
    overnight_points = [d for d in date_range if d >= start_time and d <= end_time]
    return overnight_points





if __name__=='__main__':
    start_time = '2023-01-02 21:00:00'
    end_time = '2023-01-05 22:00:00'

    # Convert them to pandas datetime objects
    start_time = pd.to_datetime(start_time,utc=True)
    end_time = pd.to_datetime(end_time,utc=True)

    print(get_overnight(start_time,end_time))

