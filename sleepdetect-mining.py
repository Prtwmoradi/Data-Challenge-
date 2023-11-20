#importing libraries
from pandasql import sqldf
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from tabulate import tabulate
import plotly.graph_objs as go

#testing on 1 series_id
train_series = pd.read_csv(r"C:\Users\Perry\OneDrive\Desktop\train_series.csv")
train_events = pd.read_csv(r"C:\Users\Perry\OneDrive\Desktop\train_events_202311161343.csv")
#showing some of our data
series_copy = train_series.copy()
events_copy = train_events.copy()

series_ids = train_events['series_id'].unique()
len(series_ids)

import matplotlib.pyplot as plt

new_train_series_list = {}
for series_id in series_ids:
    print('\n'+'='*50)
    print(series_id)
    
    _train_series = series_copy[series_copy['series_id']==series_id]
    _train_events = events_copy[events_copy['series_id']==series_id]
    
    _train_series.index = pd.to_datetime(_train_series['timestamp'], utc=True)#indexing means interchanging the column for standardisation
    
    _train_events.index = pd.to_datetime(_train_events['timestamp'], utc=True)
    
    _train_series = _train_series.assign(awake=-1, event_id=-1, night=0)
    _train_series
    current_event_time = _train_series.index[0] #this is where we have relocated the index of timestamp
    for _event_id, _events in enumerate(_train_events.itertuples()):
        _event = 0 if _events.event=='onset' else 1
        if _event_id == (len(_train_events)-1):
            _event = -1
        
        if not pd.isnull(_events.step):
            _train_series.loc[_train_series.index>=_events.Index, ['awake']] = _event
            current_event_time = _events.Index
        else:
            _train_series.loc[_train_series.index>=current_event_time, ['awake']] = -1
        _train_series.loc[_train_series.index>=_events.Index, ['event_id']] = _event_id
        _train_series.loc[_train_series.index>=_events.Index, ['night']] = _events.night
        
    new_train_series_list[series_id] = _train_series.copy()
    
    fig, ax = plt.subplots(figsize=(20, 3))
    _plot_series = _train_series['angelz']
    _plot_series.plot(ax=ax)
    (_train_series['awake']*_plot_series.abs().max()*0.5+_plot_series.abs().max()*0.5).plot(ax=ax, linewidth=5)
    for _time, _event in zip(_train_events.index, _train_events['event']):
        if not pd.isnull(_time):
            ax.axvline(x=_time, color='red' if _event=='onset' else 'green')
    plt.show()
