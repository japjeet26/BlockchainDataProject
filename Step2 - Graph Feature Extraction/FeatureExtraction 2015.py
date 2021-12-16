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


## Get the ransomware addresses from BitcoinHeist
df_ransomware_addr = pd.read_csv(r'C:\Users\japje\BDAProj\Working iGraph\ransomware_addresses_bitcoinHeist.csv')
set_ransomware = set(list(df_ransomware_addr))

##  Read the graph created in previous step for future extraction
graph_all = ig.Graph.Read_Picklez(r'C:\Users\japje\BDAProj\Working iGraph\Graph2015.gzip')




max_day = max(filter(None,[x['day'] for x in graph_all.vs]))
min_day = min(filter(None,[x['day'] for x in graph_all.vs]))

## List that will store features extracted for different days within the loop
all_days = []

for day in range(min_day, max_day+1):   
    
    ## This dictionary will store the simple addres features (5 in total)
    ## Default dictionary with default value = [0,0,0,0]
    addr_props = defaultdict(lambda : [0,0,0,0,0])
    
    ## Subgraph extracted for a given day in loop
    g_day = graph_all.subgraph(graph_all.vs.select(day_eq = day))
    
    ## If no node in given day, just go to next iteration
    if(len(g_day.vs) == 0):
        continue
    
    ## This set tracks the non-ransomware addresses covered in a given day
    ## Loop cuts if it exceed 2000
    address_covered = set()
    
    ###############################################
    #  Find Starter Transactions
    ###############################################
    list_starter_txs = []
    for v in g_day.vs.select(type_eq = True):
        if (v.indegree() == 0):
            list_starter_txs.append(v.index)
    
    ###############################################
    # Address Feature Extraction (Simple features)
    # 1. Total In-degree, 2. Total out-degree, 
    # 3. Total In-amount, 4. Total out-amount
    ###############################################
    ## Loop through Output nodes of given day
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
        # Logic used - Find shortest path between every
        # starter node and given output node - if empty,
        # that implies no path exist (using iGraph inbuilt method)
        # Otherwise, path exist and we increment
        ###############################################
        for t in list_starter_txs:
            if(g_day.get_shortest_paths(t, v.index)!=[[]]):
                addr_props[address][4] += 1
        
        
        ## to limit non-ransomware outputs to 2000 per day
        if(not v['address'] in set_ransomware):
            address_covered.add(address)
        if len(address_covered) > 2000:
            break
    
    
    ##############################################################################
    # BitcoinHeist Feature - Weight
    ## Calculated separately from other features
    ## The loop does a BFS traversal from all the starter transactions
    ## The weights are calculated for each output address traversed for previous features
    ##############################################################################
    ## Default weight assigned is 1 for each starter transaction
    address_weights = defaultdict(lambda : [1]*len(list_starter_txs) )
    for i in range(len(list_starter_txs)):
        for node in g_day.bfsiter(vid = list_starter_txs[i], mode = 'out'):
            if(not node['type'] and node['address'] in address_covered):
                weight_delta = 1/len(g_day.successors(g_day.predecessors(node.index)[0]))
                address_weights[node['address']][i] *= weight_delta
    
    ## Summing up for all starter transactions to get weight for each address                
    for k in address_weights:
        address_weights[k] = sum(address_weights[k])
    ## Convert dictionary to pandas Dataframe
    df_weights = pd.DataFrame({'address':address_weights.keys(), 'weight (BTCH)':address_weights.values()})
   
    ### Generate a Pandas data frame of address features for given day
    df_features = pd.DataFrame(np.concatenate(list(addr_props.values())).reshape(-1,5), columns = ['sum in-degree', 'sum out-degree (BTCH)', 'sum in-amount', 'sum out-amount (BTCH)', 'count (BTCH)'])
    df_features['address'] = addr_props.keys()
    df_features['day'] = day
    ## Merging with the separate DataFrame having Weight feature
    df_features = df_features.merge(df_weights, on = 'address', how = 'left')
    ## Add day's feature to list
    all_days.append(df_features)

## Combining different days features into a single DataFrame
df_all_features = pd.concat(all_days)
df_all_features['weight (BTCH)'].fillna(0, inplace = True)

#### Inserting Labels from Bitcoin Heist dataset
## Read BitcoinHeist complete Data
df_bitcoin_heist = pd.read_csv(r'C:\Users\japje\BDAProj\BitcoinHeist\BitcoinHeistData.csv')
## Extract Address-Label mapping
label_df = df_bitcoin_heist[['address','label']].drop_duplicates()
## Delete Bitcoinheist dataframe to save memory
del df_bitcoin_heist

## Merging Label mapping with Features data
df_all_features = df_all_features.merge(label_df, on = 'address', how = 'left')
## Many addresses not found in the Bitcoin Heist Dataset 
## Resulting in NA values for label, we replace them with 0
df_all_features['label'].fillna(0, inplace = True)
## Add the final label
## "0" for White and NAs
## "1" for other ransomware categories
df_all_features['label_final'] = np.where(df_all_features['label'].isin([0, 'white']), 0, 1 )
df_all_features.to_csv(r'C:\Users\japje\BDAProj\Working iGraph/features_2015_new.csv', index=False, header=True, columns = ['day', 'address', 'sum in-degree', 'sum out-degree (BTCH)', 'sum in-amount', 'sum out-amount (BTCH)', 'count (BTCH)', 'weight (BTCH)', 'label', 'label_final'])
