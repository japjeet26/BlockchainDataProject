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


## Creating the mapping
## 1. transactionHash- Integer ID mapping

list_tx_hashes = []
path = r"C:\Users\japje\BDAProj\Sample2015"
for root, dir, files in os.walk(path):
    for f in files:
        with open(os.path.join(root,f), encoding = "utf-8") as f:
            for line in f:
                ## Split the row
                cells = line.split("\t")

                ## Add transaciton to Set
                transaction_hash = cells[1]
                
                list_tx_hashes.append([transaction_hash])
                
                ## in case of input files also add txn hash of inputs to mapping
                list_ip_txns = cells[2::2]
                
                list_tx_hashes.append(list_ip_txns)
            
list_tx_hashes = [item for sublist in list_tx_hashes for item in sublist]                
list_tx_hashes = set(list_tx_hashes)
txn_mapping = dict(zip(list_tx_hashes, range(len(list_tx_hashes))))
del list_tx_hashes

## Creating transaction - Unix time mapping

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
            

## Creating the mapping



## Generation of output mapping
## 1. txnID -> [OutputIDs]
## and output data
## 2. Dataframe(Index(OutputID), Columns(addressID, amount)
## 3. Output ID --> Unix Time

list_addresses = []
path = r"C:\Users\japje\BDAProj\Sample2015"
mapping_tx_to_op = {}
# mapping_op_time = {}
data_op = []
global_index = -1
for root, dir, files in os.walk(path):
    for f in files:
        if("outputs" in f):
            with open(os.path.join(root,f), encoding = "utf-8") as f:
                for line in f:
                    ## Split the row
                    cells = line.rstrip().split("\t")
                    # print(cells)
#                     list_tx_index = list(range(0, len(cells[2:])//2))
                    list_op_id = list(range(global_index+1, global_index+1+len(cells[2:])//2))
#                     list_tx_rep = [txn_mapping[cells[1]]] * (len(cells[2:])//2)

                    list_addresses = [x for x in cells[2:][::2]]
                    list_amounts = cells[3:][::2]
                    list_time = [np.uint32(cells[0])]* (len(cells[2:])//2)
#                     mapping_delta1 = {}
#                     for x, y, z in zip(list_tx_rep, list_tx_index, list_addr_id):
#                         if x in mapping_delta1:
#                             mapping_delta1[x][y] = z
#                         else:
#                             mapping_delta1[x] = {y:z}
                   
    
                    mapping_delta1 = {txn_mapping[cells[1]]: list_op_id}

                    data_delta = np.array([list_op_id, list_addresses, list_amounts, list_time]).T.reshape(-1,4)

                    mapping_tx_to_op.update(mapping_delta1)
                    data_op.append(data_delta)
                    
#                     ## logic to get unix timestamp
#                     unix_time = np.uint32(cells[0])
#                     mapping_delta_t = zip(list_addr_id, [unix_time] * len(list_addr_id))
#                     mapping_op_time.update(mapping_delta_t)
                    
                    global_index = global_index+1
                global_index = global_index+1

data_op = pd.DataFrame(np.concatenate(data_op), columns= ['op_id', 'addr_id', 'amt', 'time' ])
# data_op['addr_id'] = data_op['addr_id'].astype(np.uint32)
data_op['amt'] = data_op['amt'].astype(np.uint64)
data_op['op_id'] = data_op['op_id'].astype(np.uint32)
data_op['time'] = data_op['time'].astype(np.uint32)
data_op = data_op.set_index('op_id')


## Reading inputs to create input mapping {transactionID --> [output indexes]}
## Adding edges

###### Testing #####

count_match = 0 ### To track matched inputs (i.e. corresponding UTXO created same year)
count_no_match = 0
####################


path = r"C:\Users\japje\BDAProj\Sample2015"
# mapping_tx_to_op = {}
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
#                             if(int(txn_mapping[inputs[i]]) in mapping_tx_to_op):
#                                 print(str(txn_mapping[inputs[i]]) + "----" + str(inputs[i+1]))
                            input_index.append(mapping_tx_to_op[int(txn_mapping[inputs[i]])][int(inputs[i+1])])
                            count_match += 1
#                             else:
                            # (TODO) Add code to handle missing addresses - not found within this year
#                                 count_no_match += 1
#                                 input_index.append(-1)
                        else:
                            count_no_match += 1
                            input_index.append(-1)
                        i = i+2
                    mapping_ip[txn_id] = input_index



## Graph creation using iGraph
from igraph import *
## Extracting memory efficent temporal variable (granularity = daily) from Unix time
## Calculation: (UnixTimeofTxn - 1230962400)/(no. of seconds in a day)
## Here 1230962400 is Unix time for Jan 03, 2009 00:00:00 (begining of the day when genesis block was created)


converted_times = list(map(lambda x: np.uint16(math.floor((float(x)-float(1230962400))/(60*60*24))), list(mapping_tx_time.values())))
converted_times_op = list(map(lambda x: np.uint16(math.floor((float(x)-float(1230962400))/(60*60*24))), list(data_op['time'])))

## 1. Add transaction nodes to graph
# 1.1 - Initialize
graph2010 = Graph(n = len(mapping_tx_time), directed = True )

# 1.2 - Add attributes - i) Transaction ID, ii) Day, iii) Year - to the transaction vertices
graph2010.vs['t_id'] = list(map(int, mapping_tx_time.keys()))
graph2010.vs['day'] = converted_times
graph2010.vs['type'] = True ## Node type - True for transaction nodes, False for Output nodes

#########  IMPORTANT!!! ########
## Save a mapping from transaction ID (attribute of vertex) to the actual vertex id assigned by iGraph
## as the vertex id is assigned deterministically i.e. range(0, len(no. of vertices) - 1), we can map 
## the ids at point of node additions. If there are no deletions, things are very simple!!

txn_graph_mapping = dict(zip(mapping_tx_time.keys(), range(len(mapping_tx_time))))

## Generating the bitcoin transaction network with iGraph
## Step 2.1 - Add output nodes

list_op_ID = list(data_op.index)
list_op_time = converted_times_op
list_op_amt = list(data_op['amt'])
list_op_addr = list(data_op['addr_id'])

#########  IMPORTANT!!! ########
#### Save op nodes mapping beforew adding new nodes (easy if you know no. of nodes at present) ####

next_node_id = len(graph2010.vs)
num_new_nodes = len(list_op_ID)
op_graph_mapping = dict(zip(list_op_ID, range(next_node_id, next_node_id + num_new_nodes)))


## Now add vertices
graph2010.add_vertices(len(list_op_ID), attributes={'id':list_op_ID, 'day':list_op_time, 'amount':list_op_amt, 'address':list_op_addr, 'type':False})

## Add special vertex to handle outputs from prev yrs (op_id = -1)
graph2010.add_vertices(1, attributes={'id': -1})

##add this to graph mapping
op_graph_mapping[-1] = len(graph2010.vs) -1


## 3.1 - Add output edges
## Key error to handle in case input transaction in previous year

edge_list_op = [(txn_graph_mapping[p],op_graph_mapping[q]) for p in mapping_tx_to_op for q in mapping_tx_to_op[p] if (p in mapping_tx_to_op and q in mapping_tx_to_op[p]) ]


graph2010.add_edges(edge_list_op)



## 3.2 add input edges from mapping - "mapping_ip "
## This completes graph creation
## Key error to handle in case input transaction in previous year

edge_list_ip = [(op_graph_mapping[q], txn_graph_mapping[p]) for p in mapping_ip for q in mapping_ip[p]]


graph2010.add_edges(edge_list_ip)


### Serialize the graph

graph2010.write_picklez(fname = r'C:\Users\japje\BDAProj\Working iGraph/Graph2015.gzip')