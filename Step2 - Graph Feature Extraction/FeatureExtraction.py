# -*- coding: utf-8 -*-
"""
Created on Sat Dec  4 19:01:21 2021

@author: japje
"""
import igraph as ig
import pandas as pd
import os
import csv
from collections import defaultdict
import numpy as np


## Get the ransomware addresses from bitcoinHeist
df_ransomware_addr = pd.read_csv(r'C:\Users\japje\BDAProj\Working iGraph\ransomware_addresses_bitcoinHeist.csv')
set_ransomware = set(list(df_ransomware_addr))

#graph = "read graph here"
graph_all = ig.Graph.Read_Picklez(r'C:\Users\japje\BDAProj\Working iGraph\Graph2010.gzip')


all_days = []
## Default dictionary with default value = [0,0,0,0]
addr_props = defaultdict(lambda : [0,0,0,0])
for day in range(393, 727):
    
    ## to limit non-ransomware addresses to 2000 per day
    count_non_ransomware_day = 0
    g_day = graph_all.subgraph(graph_all.vs.select(day_eq = day))
    for v in g_day.vs.select(type_eq = False):
        if (not v['type']):
            in_deg = v.indegree()
            out_deg = v.outdegree()
            in_amount = v['amount']
            out_amount = in_amount if out_deg > 0 else 0
            addr_props[v["address"]][0] += in_deg
            addr_props[v["address"]][1] += out_deg
            addr_props[v["address"]][2] += in_amount
            addr_props[v["address"]][3] += out_amount
            if(not v['address'] in set_ransomware):
                count_non_ransomware_day += 1
            if count_non_ransomware_day >= 2000:
                break
    ### Pandas data frame of address features for given day
    df_features = pd.DataFrame(np.concatenate(list(addr_props.values())).reshape(-1,4), columns = ['sum in-degree', 'sum out-degree', 'sum in-amount', 'sum out-amount'])
    df_features['address Id'] = addr_props.keys()
    df_features['day'] = day
    
    all_days.append(df_features)
 
df_all_features = pd.concat(all_days)

### Add the ransomware/non-ransomware flag randomly (5-95)

label_df = pd.DataFrame(list(set(df_all_features['address Id'])), columns= ['address Id'])
label_df['label'] = np.random.choice([0, 1], size=len(label_df.index), p=[.95, .05])

df_all_features = df_all_features.merge(label_df, on = 'address Id', how = 'left')
df_all_features.to_csv(r'C:\Users\japje\BDAProj\Working iGraph/features_2010.csv')