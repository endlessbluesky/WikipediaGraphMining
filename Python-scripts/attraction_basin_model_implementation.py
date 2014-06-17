#!/usr/bin/env python
# -*- coding: utf-8 -*-
import graphlab as gl
import pymongo
client = pymongo.MongoClient(host='idata-a.d1.comp.nus.edu.sg', port=27017)
#print(help(pymongo.MongoClient))
page = client['wiki']['page']
page_cursor = page.find()

g = gl.SGraph()

vertices = gl.SFrame.read_csv('http://s3.amazonaws.com/GraphLab-Datasets/bond/bond_vertices.csv')
edges = gl.SFrame.read_csv('http://s3.amazonaws.com/GraphLab-Datasets/bond/bond_edges.csv')

g = g.add_vertices(vertices=vertices, vid_field='name')
g = g.add_edges(edges=edges, src_field='src', dst_field='dst')

print('------trying simple graph query, i.e., select all pairs of friends------')
print(g.get_edges(fields = {'relation': 'friend'}))
print('------trying simple graph analytics, i.e., page-rank scoring------')

"""
--------------------------------------------------------
dir(gl) = 
['DeprecationHelper', 'Edge', 'Graph', 
'Model', 'SArray', 'SFrame', 'SGraph', 
'Sketch', 'Vertex', 
'__VERSION__', '__builtins__', 
'__doc__', '__file__', '__name__', 
'__package__', '__path__', '_launch', 
'_stop', 'aggregate', 'aws', 'clustering', 
'connect', 'connected_components', 
'cython', 'data_structures', 'evaluation', 
'get_newest_version', 'graph_analytics', 
'graph_coloring', 'graphlab', 'kcore', 
'kmeans', 'linear_regression', 'load_graph', 
'load_model', 'load_sframe', 'logistic_regression', 
'pagerank', 'perform_version_check', 
'product_key', 'recommender', 'shortest_path', 
'toolkits', 'triangle_counting', 'util', 
'version', 'version_info', 'vowpal_wabbit']
--------------------------------------------------------
"""

page_rank_model = gl.pagerank.create(g)
print(type(page_rank_model))
print(page_rank_model.get('pagerank').topk(column_name='pagerank'))