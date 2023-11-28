#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tabulate import tabulate
import plotly.graph_objs as go
import missingno as msno
import matplotlib.pyplot as plt
from scipy.stats import pearsonr


# In[2]:


# Update file paths with double backslashes or raw strings
train_events_path = r"C:\Users\Perry\OneDrive\Desktop\child-mind-institute-detect-sleep-states\child-mind-institute-detect-sleep-states\train_events.csv"
train_series_path = r"C:\Users\Perry\OneDrive\Desktop\child-mind-institute-detect-sleep-states\child-mind-institute-detect-sleep-states\train_series.parquet"

# Read data from CSV and Parquet files
train_events = pd.read_csv(train_events_path)
train_series = pd.read_parquet(train_series_path)


# In[3]:


train_events.info()
train_series.info()


# In[4]:


series_ids = train_events['series_id'].unique()
print(len(series_ids))


# In[5]:


series_copy = train_series.copy()
events_copy = train_events.copy()


# In[10]:


series_id1 = '038441c925bb'
series_id2 = '03d92c9f6f8a'

# Extract data for the first series_id
_train_series1 = series_copy[series_copy['series_id'] == series_id1]
_train_series1.index = pd.to_datetime(_train_series1['timestamp'], utc=True)

# Extract data for the second series_id
_train_series2 = series_copy[series_copy['series_id'] == series_id2]
_train_series2.index = pd.to_datetime(_train_series2['timestamp'], utc=True)

# Calculate correlation coefficients for the two series_id values
correlation_coefficient1, p_value1 = pearsonr(_train_series1['anglez'], _train_series1['enmo'])
correlation_coefficient2, p_value2 = pearsonr(_train_series2['anglez'], _train_series2['enmo'])

# Plotting the correlation coefficients on a linear graph
plt.plot([series_id1, series_id2], [correlation_coefficient1, correlation_coefficient2], marker='o', linestyle='-')
plt.xlabel('Series ID')
plt.ylabel('Correlation Coefficient')
plt.title('Correlation Coefficients for Two Series IDs')
plt.grid(True)
plt.show()

# Print correlation information
print(f"Correlation coefficient for {series_id1}: {correlation_coefficient1}")
print(f"P-value for {series_id1}: {p_value1}")
print(f"Correlation coefficient for {series_id2}: {correlation_coefficient2}")
print(f"P-value for {series_id2}: {p_value2}")


# In[13]:


series_id1 = '038441c925bb'
series_id2 = '03d92c9f6f8a'

# Extract data for the first series_id
_train_series1 = series_copy[series_copy['series_id'] == series_id1]
_train_series1.index = pd.to_datetime(_train_series1['timestamp'], utc=True)

# Extract data for the second series_id
_train_series2 = series_copy[series_copy['series_id'] == series_id2]
_train_series2.index = pd.to_datetime(_train_series2['timestamp'], utc=True)

# Calculate correlation coefficients for the two series_id values
correlation_coefficient1, p_value1 = pearsonr(_train_series1['anglez'], _train_series1['enmo'])
correlation_coefficient2, p_value2 = pearsonr(_train_series2['anglez'], _train_series2['enmo'])

# Plotting the correlation coefficients on a linear graph
plt.plot([0, 1], [correlation_coefficient1, correlation_coefficient2], marker='o', linestyle='-')
plt.xticks([0, 1], [series_id1, series_id2])  # Set tick labels
plt.xlabel('Series ID')
plt.ylabel('Correlation Coefficient')
plt.title('Correlation Coefficients for Two Series IDs')
plt.grid(True)
plt.show()

# Print correlation information
print(f"Correlation coefficient for {series_id1}: {correlation_coefficient1}")
print(f"P-value for {series_id1}: {p_value1}")
print(f"Correlation coefficient for {series_id2}: {correlation_coefficient2}")
print(f"P-value for {series_id2}: {p_value2}")


# In[8]:


correlation_dict = {}

for series_id in series_ids:
    print('\n' + '=' * 50)
    print(series_id)

    _train_series = series_copy[series_copy['series_id'] == series_id]
    _train_series.index = pd.to_datetime(_train_series['timestamp'], utc=True)

    # Assuming 'anglez' and 'enmo' are columns in your DataFrame
    correlation_coefficient, p_value = pearsonr(_train_series['anglez'], _train_series['enmo'])

    # Store correlation coefficient in the dictionary
    correlation_dict[series_id] = correlation_coefficient

    # Print correlation information
    print(f"Pearson Correlation Coefficient: {correlation_coefficient}")
    print(f"P-value: {p_value}")

# Display the correlation coefficients for all series_ids
print("\nCorrelation Coefficients:")
for series_id, correlation_coefficient in correlation_dict.items():
    print(f"Series ID {series_id}: {correlation_coefficient}")


# In[16]:


# Specify the two series_id values you want to compare
series_id1 = '038441c925bb'
series_id2 = '0402a003dae9'
# Dictionary to store new_train_series for the specified series IDs
new_train_series_list = {}

for series_id in [series_id1, series_id2]:
    print('\n' + '=' * 50)
    print(series_id)

    _train_series = series_copy[series_copy['series_id'] == series_id]
    _train_events = events_copy[events_copy['series_id'] == series_id]

    _train_series.index = pd.to_datetime(_train_series['timestamp'], utc=True)
    _train_events.index = pd.to_datetime(_train_events['timestamp'], utc=True)

    _train_series = _train_series.assign(awake=-1, event_id=-1, night=0)

    current_event_time = _train_series.index[0]

    for _event_id, _events in enumerate(_train_events.itertuples()):
        _event = 0 if _events.event == 'onset' else 1
        if _event_id == (len(_train_events) - 1):
            _event = -1

        if not pd.isnull(_events.step):
            _train_series.loc[_train_series.index >= _events.Index, ['awake']] = _event
            current_event_time = _events.Index
        else:
            _train_series.loc[_train_series.index >= current_event_time, ['awake']] = -1

        _train_series.loc[_train_series.index >= _events.Index, ['event_id']] = _event_id
        _train_series.loc[_train_series.index >= _events.Index, ['night']] = _events.night

    new_train_series_list[series_id] = _train_series.copy()

    # Plotting the time series data for visualization
    fig, ax = plt.subplots(figsize=(20, 4))
    _plot_series = _train_series['anglez']
    _plot_series.plot(ax=ax)
    (_train_series['awake'] * _plot_series.abs().max() * 0.5 + _plot_series.abs().max() * 0.5).plot(ax=ax, linewidth=5)
    for _time, _event in zip(_train_events.index, _train_events['event']):
        if not pd.isnull(_time):
            ax.axvline(x=_time, color='red' if _event == 'onset' else 'green')
    plt.show()


# In[14]:


series_id1 = '038441c925bb'
series_id2 = '03d92c9f6f8a'

# Extract data for the first series_id
_train_series1 = series_copy[series_copy['series_id'] == series_id1]
_train_series1.index = pd.to_datetime(_train_series1['timestamp'], utc=True)

# Extract data for the second series_id
_train_series2 = series_copy[series_copy['series_id'] == series_id2]
_train_series2.index = pd.to_datetime(_train_series2['timestamp'], utc=True)

# Calculate correlation coefficients for the two series_id values
correlation_coefficient1, p_value1 = pearsonr(_train_series1['anglez'], _train_series1['enmo'])
correlation_coefficient2, p_value2 = pearsonr(_train_series2['anglez'], _train_series2['enmo'])

# Plotting the correlation coefficients on a linear graph
plt.plot([0, 1], [correlation_coefficient1, correlation_coefficient2], marker='o', linestyle='-')
plt.xticks([0, 1], [series_id1, series_id2])  # Set tick labels
plt.xlabel('Series ID')
plt.ylabel('Correlation Coefficient')
plt.title('Correlation Coefficients for Two Series IDs')
plt.grid(True)
plt.show()

# Print correlation information
print(f"Correlation coefficient for {series_id1}: {correlation_coefficient1}")
print(f"P-value for {series_id1}: {p_value1}")
print(f"Correlation coefficient for {series_id2}: {correlation_coefficient2}")
print(f"P-value for {series_id2}: {p_value2}")


# In[ ]:




