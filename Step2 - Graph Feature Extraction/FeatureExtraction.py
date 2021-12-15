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

max_day = max(filter(None,[x['day'] for x in graph_all.vs]))
min_day = min(filter(None,[x['day'] for x in graph_all.vs]))


for day in range(min_day, max_day+1):   
   ## Track of non-ransomware outputs in given day
    count_non_ransomware_day = 0
    
    ## This dictionary will store the simple addres features (4 in total)
    addr_props = defaultdict(lambda : [0,0,0,0,0])
        
    g_day = graph_all.subgraph(graph_all.vs.select(day_eq = day))
    
    if(len(g_day.vs) == 0):
        continue
    
    ###############################################
    #  Find Starter Transactions
    ###############################################
    list_starter_txs = []
    for v in g_day.vs.select(type_eq = True):
        if (v.indegree() == 0):
            list_starter_txs.append(v.index)
    
    ###############################################
    # Feature Extraction (Simple features)
    ###############################################
    address_covered = set()

    for v in g_day.vs.select(type_eq = False):
        in_deg = v.indegree()
        out_deg = v.outdegree()
        in_amount = v['amount']
        out_amount = in_amount if out_deg > 0 else 0
        address = v["address"]
        addr_props[address][0] += in_deg
        addr_props[address][1] += out_deg
        addr_props[address][2] += in_amount
        addr_props[address][3] += out_amount
        
        ###############################################
        # BitcoinHeist Feature - Count
        ###############################################

        for t in list_starter_txs:
            if(g_day.get_shortest_paths(t, v.index)!=[[]]):
                addr_props[address][4] += 1
        
        
        ## to limit non-ransomware outputs to 2000 per day
        if(not v['address'] in set_ransomware):
            address_covered.add(address)
        if len(address_covered) > 2000:
            break
    
    
    ###############################################
    # BitcoinHeist Feature - Weight
    ## Calculated separately from other features
    ## The loop starts from different starter transactions and does a BFS traversal
    ## The weights are calculated for each address which was part of other features
    ###############################################
    address_weights = defaultdict(lambda : [1]*len(list_starter_txs) )
    for i in range(len(list_starter_txs)):
        for node in g_day.bfsiter(vid = list_starter_txs[i], mode = 'out'):
            if(not node['type'] and node['address'] in address_covered):
                weight_delta = 1/len(g_day.successors(g_day.predecessors(node.index)[0]))
                address_weights[node['address']][i] *= weight_delta
                    
    for k in address_weights:
        address_weights[k] = sum(address_weights[k])
    
    df_weights = pd.DataFrame({'address':address_weights.keys(), 'weight (BTCH)':address_weights.values()})
    


        
    ### Pandas data frame of address features for given day
    df_features = pd.DataFrame(np.concatenate(list(addr_props.values())).reshape(-1,5), columns = ['sum in-degree', 'sum out-degree (BTCH)', 'sum in-amount', 'sum out-amount (BTCH)', 'count (BTCH)'])
    df_features['address'] = addr_props.keys()
    df_features['day'] = day
    
    df_features = df_features.merge(df_weights, on = 'address', how = 'left')
    
    all_days.append(df_features)
 
df_all_features = pd.concat(all_days)

### Add the ransomware/non-ransomware flag randomly (5-95)

label_df = pd.DataFrame(list(set(df_all_features['address'])), columns= ['address'])
label_df['label'] = np.random.choice([0, 1], size=len(label_df.index), p=[.95, .05])

df_all_features = df_all_features.merge(label_df, on = 'address', how = 'left')
df_all_features['weight (BTCH)'].fillna(0, inplace = True)
df_all_features.to_csv(r'C:\Users\japje\BDAProj\Working iGraph/features_2010_new.csv', index=False, header=True, columns = ['day', 'address', 'sum in-degree', 'sum out-degree (BTCH)', 'sum in-amount', 'sum out-amount (BTCH)', 'count (BTCH)', 'weight (BTCH)', 'label'])
