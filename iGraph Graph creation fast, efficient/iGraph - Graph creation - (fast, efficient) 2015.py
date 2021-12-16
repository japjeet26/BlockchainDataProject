# -*- coding: utf-8 -*-
"""
Created on Sat Nov 20 23:33:22 2021

@author: japje
"""

import os
import pickle
import pandas as pd
import numpy as np
import sys
import math


########################
## Generating Mappings
########################


## Creating the transaction mapping
## 1. transactionHash- Integer ID mapping

## Initialize the list of transaction hashes
list_tx_hashes = []

## Traverse all text files - both input and output
## Parse line by line and extract all the transaction hashes
## From input files - Hash of input transaction as well as output transactions
## From output files - Hash of parent transactions
## Add the extracted transaction hash to list "list_tx_hashes"

path = r"C:\Users\japje\BDAProj\Sample2015"
for root, dir, files in os.walk(path):
    for f in files:
        with open(os.path.join(root,f), encoding = "utf-8") as f:
            for line in f:
                ## Split the row
                cells = line.split("\t")
                ## Add transaciton hash to List
                transaction_hash = cells[1]
                list_tx_hashes.append([transaction_hash])
                ## in case of input files also add txn hash of inputs to mapping
                if "input" in f:
                    list_ip_txns = cells[2::2]
                    list_tx_hashes.append(list_ip_txns)
            
## flatten the list
list_tx_hashes = [item for sublist in list_tx_hashes for item in sublist]                
## TypeCast to "set" data structure
list_tx_hashes = set(list_tx_hashes)
## Create a mapping from set - By assigning the IDs to transactions starting from 0
txn_mapping = dict(zip(list_tx_hashes, range(len(list_tx_hashes))))
## Memory management - Detete the original list
del list_tx_hashes

## Creating {Transaction ID - Unix timestamp} mapping
## Traverse the text files line by line
## Parse timestamp and transaction hash
## Use the previous mapping and map each integer Transaction ID to respective timestamp
mapping_tx_time = {}
for root, dir, files in os.walk(path):
    for f in files:
        with open(os.path.join(root,f), encoding = "utf-8") as f:
            for line in f:
                cells = line.rstrip().split("\t")
                txn_id = txn_mapping[cells[1]]
                unix_time = np.uint32(cells[0])
                mapping_tx_time[txn_id] = unix_time
        print("done " + str(f))
            

## Generation of mappings from output text files
## Note: Output ID is assigned to each distinct output
## 1. "mapping_tx_to_op" {txnID -> [OutputIDs]}
## 2. "data_op" Dataframe with output data -->(Index(OutputID), Columns(addressID, amount)
## Note: We use mapping
list_addresses = []
path = r"C:\Users\japje\BDAProj\Sample2015"
mapping_tx_to_op = {}
data_op = []
global_index = -1
for root, dir, files in os.walk(path):
    for f in files:
        if("outputs" in f):
            with open(os.path.join(root,f), encoding = "utf-8") as f:
                for line in f:
                    ## Split the row
                    cells = line.rstrip().split("\t")
                    list_op_id = list(range(global_index+1, global_index+1+len(cells[2:])//2))
                    list_addresses = [x for x in cells[2:][::2]]
                    list_amounts = cells[3:][::2]
                    list_time = [np.uint32(cells[0])]* (len(cells[2:])//2)
                    mapping_delta = {txn_mapping[cells[1]]: list_op_id}
                    data_delta = np.array([list_op_id, list_addresses, list_amounts, list_time]).T.reshape(-1,4)
                    mapping_tx_to_op.update(mapping_delta)
                    data_op.append(data_delta)
                    global_index = global_index+1
                global_index = global_index+1
## Generating Pandas dataframe - "data_op" from the list of numpy arrays
data_op = pd.DataFrame(np.concatenate(data_op), columns= ['op_id', 'addr_id', 'amt', 'time' ])
data_op['amt'] = data_op['amt'].astype(np.uint64)
data_op['op_id'] = data_op['op_id'].astype(np.uint32)
data_op['time'] = data_op['time'].astype(np.uint32)
data_op = data_op.set_index('op_id')


## Generation of mappings from Input text files
## Note: Output ID is assigned to each distinct output
## 1. "mapping_tx_to_op" {txnID -> [OutputIDs]}
## 2. "data_op" Dataframe with output data -->(Index(OutputID), Columns(addressID, amount)
## 3. Output ID --> Unix Time
path = r"C:\Users\japje\BDAProj\Sample2015"
mapping_ip = {}
for root, dir, files in os.walk(path):
    for f in files:
        if("inputs" in f):
            # print(f)
            with open(os.path.join(root,f), encoding = "utf-8") as f:
                for line in f:
                    cells = line.rstrip().split("\t")
                    txn_id = txn_mapping[cells[1]]
                    
                    inputs = cells[2:]
                    input_index = []
                    for i in range(0, len(inputs)):
                        # print(str(inputs[i]) + "----"+str(inputs[i+1]))
                        if(inputs[i] in txn_mapping and int(txn_mapping[inputs[i]]) in mapping_tx_to_op):
                            input_index.append(mapping_tx_to_op[int(txn_mapping[inputs[i]])][int(inputs[i+1])])
                        else:
                            input_index.append(-1)
                        i = i+2
                    mapping_ip[txn_id] = input_index


########################
## Graph Generation
## (Using the mappings generated above)
########################


from igraph import *


## Extracting memory efficent temporal variable (granularity = daily) from Unix time
## Calculation: (UnixTimeofTxn - 1230962400)/(no. of seconds in a day)
## Here 1230962400 is Unix time for Jan 03, 2009 00:00:00 (begining of the day when genesis block was created)
converted_times = list(map(lambda x: np.uint16(math.floor((float(x)-float(1230962400))/(60*60*24))), list(mapping_tx_time.values())))
converted_times_op = list(map(lambda x: np.uint16(math.floor((float(x)-float(1230962400))/(60*60*24))), list(data_op['time'])))

## 1. Add transaction nodes to graph
# 1.1 - Initialize
graphBTC = Graph(n = len(mapping_tx_time), directed = True )

# 1.2 - Add node attributes - i) Transaction ID, ii) Day (as defined above), iii) Type = True - to the transaction vertices
# "type == True" means transaction node, while "type = False" means Output node
graphBTC.vs['t_id'] = list(map(int, mapping_tx_time.keys()))
graphBTC.vs['day'] = converted_times
graphBTC.vs['type'] = True 

#########  IMPORTANT!!! ########
## Save a mapping from transaction ID (attribute of vertex) to the actual vertex id assigned by iGraph
## as the vertex id is assigned deterministically i.e. range(0, len(no. of vertices) - 1), we can map 
## the ids at point of node additions. If there are no deletions, things are very simple!!

txn_graph_mapping = dict(zip(mapping_tx_time.keys(), range(len(mapping_tx_time))))

## Generating the bitcoin transaction network with iGraph
## Step 2.1 - Add output nodes

## Generating lists of attributes first
list_op_ID = list(data_op.index)
list_op_time = converted_times_op
list_op_amt = list(data_op['amt'])
list_op_addr = list(data_op['addr_id'])


#### Save output nodes mapping before adding new nodes (easy if you know number of current nodes) ####
next_node_id = len(graphBTC.vs)
num_new_nodes = len(list_op_ID)
op_graph_mapping = dict(zip(list_op_ID, range(next_node_id, next_node_id + num_new_nodes)))

## Now add vertices
graphBTC.add_vertices(len(list_op_ID), attributes={'id':list_op_ID, 'day':list_op_time, 'amount':list_op_amt, 'address':list_op_addr, 'type':False})
## Add special vertex to handle outputs from prev yrs (op_id = -1)
graphBTC.add_vertices(1, attributes={'id': -1})
op_graph_mapping[-1] = len(graphBTC.vs) -1

## Step 3 - Add Edges to graph
## 3.1 - Adding output Edges (From transaction nodes to their generated output nodes)
## Generate edge list of the form [(u1,v1), (u2,v2) ...] where an edge is tuple of node ids
edge_list_op = [(txn_graph_mapping[p],op_graph_mapping[q]) for p in mapping_tx_to_op for q in mapping_tx_to_op[p] if (p in mapping_tx_to_op and q in mapping_tx_to_op[p]) ]
## Insert all edges to graph
graphBTC.add_edges(edge_list_op)

## 3.2 Adding Input Edges (From UTXO Output nodes to Transaction nodes)
edge_list_ip = [(op_graph_mapping[q], txn_graph_mapping[p]) for p in mapping_ip for q in mapping_ip[p]]
graphBTC.add_edges(edge_list_ip)


### Serialize the graph in compressed form
graphBTC.write_picklez(fname = r'C:\Users\japje\BDAProj\Working iGraph/Graph2015.gzip')