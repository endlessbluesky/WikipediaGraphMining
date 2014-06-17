#import the module (running as a local server once imported)
import graphlab as gl

#the constructor gl.Graph() has deprecated
#use gl.SGraph() instead
sample_graph = gl.SGraph()

#load vertices and edges
#they are stored in disk-based SFrame data structure
#i.e., type(vertices) = type(edges) = <class 'graphlab.data_structures.sframe.SFrame'>
vertices_sframe = gl.SFrame.read_csv('http://s3.amazonaws.com/GraphLab-Datasets/bond/bond_vertices.csv')
edges_sframe = gl.SFrame.read_csv('http://s3.amazonaws.com/GraphLab-Datasets/bond/bond_edges.csv')

#now insert the vertices and edges into the graph object
#notice that calling g.add_vertices(*args) is not sufficient
#an assignment statement is needed
#take note of the required options in the methods
sample_graph = sample_graph.add_vertices(vertices = vertices_sframe, vid_field = 'name')
sample_graph = sample_graph.add_edges(edges = edges_sframe, src_field = 'src', dst_field = 'dst')

#simple edge-info-based selection query: get all pairs of friends:
#return an SFrame instance of all edges satisfying the query
print('------trying simple graph query, i.e., select all pairs of friends------')
print(sample_graph.get_edges(fields = {'relation': 'friend'}))

#simple page-rank analytics
print('------trying simple graph analytics, i.e., page-rank scoring------')
#note that the page_rank_model is of type
#<class 'graphlab.toolkits.graph_analytics.pagerank.PagerankModel'>
#and must be constructed using the ..pagerank.create(SGraph-object) method
#instead of the default constructor
page_rank_model = gl.pagerank.create(sample_graph)

#the .get('pagerank') query would return an SFrame instance 
#containing the results of the computation
page_rank_result_sframe = page_rank_model.get('pagerank')
print(page_rank_result_sframe)

#may also save the frame 
#note that the string 'page_rank_result_sframe' is the directory name
#the structure would split 
page_rank_result_sframe.save('page_rank_result_sframe')