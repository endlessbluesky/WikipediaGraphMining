import numpy
import scipy.sparse
import json
import bson
import cPickle


MAX_ID = 14313024

#a list of vertices with outgoing links
graph_data = []
graph_data.append({'_id': 1, 'links' : [3,5,2]})
graph_data.append({'_id': 2, 'links' : [1]})
graph_data.append({'_id': 3, 'links' : [2, 5]})
graph_data.append({'_id': 5, 'links' : [2, 1]})

f = open('graph_data_input', 'r')
cPickle.load(graph_data, f)
f.close()

matA = scipy.sparse.dok_matrix((MAX_ID, MAX_ID), dtype = int)
print(matA)
