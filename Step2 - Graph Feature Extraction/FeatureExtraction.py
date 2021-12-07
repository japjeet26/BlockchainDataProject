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






#graph = "read graph here"
graph_all = ig.Graph.Read_Picklez(r'C:\Users\japje\BDAProj\Working iGraph\Graph2010.gzip')


all_days = []
## Default dictionary with default value = [0,0,0,0]
addr_props = defaultdict(lambda : [0,0,0,0])
for day in range(393, 700):
    g_day = graph_all.subgraph(graph_all.vs.select(day_eq = day))
    for v in g_day.vs.select(type_eq = False):
        in_deg = v.indegree()
        out_deg = v.outdegree()
        in_amount = v['amount']
        out_amount = in_amount if out_deg > 0 else 0
        addr_props[v["address"]][0] += in_deg
        addr_props[v["address"]][1] += out_deg
        addr_props[v["address"]][2] += in_amount
        addr_props[v["address"]][3] += out_amount
    ### Pandas data frame of address features for given day
    df_features = pd.DataFrame(np.concatenate(list(addr_props.values())).reshape(-1,4), columns = ['sum in-degree', 'sum out-degree', 'sum in-amount', 'sum out-amount'])
    df_features['address Id'] = addr_props.keys()
    df_features['day'] = day
    
    all_days.append(df_features)
 
df_all_features = pd.concat(all_days)

df_all_features.to_csv(r'C:\Users\japje\BDAProj\Working iGraph/features_2010.csv')