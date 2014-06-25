import graphlab as gl 
import numpy as np
import pandas

#this script demonstrate
#modification of an SFrame instance
#by loading it into memory as a DataFrame
#remove one row
#and store it back to hard-disk
data_frame = pandas.DataFrame()
sframe = gl.SFrame(data = data_frame)

try:
    sframe = gl.load_sframe('./WikipediaGraphMining/Python-scripts/dataset')
except IOError:
    sframe = gl.SFrame.read_csv('http://testdatasets.s3-website-us-west-2.amazonaws.com/users.csv.gz',
                         delimiter=',',
                         header=False,
                         comment_char="#",
                         column_type_hints={'user_id': int})
    sframe.save('dataset')

print('------the original SFrame loaded:------')
print(sframe.head(6))

#column names: ['X1', 'X2', 'X3', 'X4', 'X5', 'X6']
column_titles = sframe.head(1)
old_new_titles = {}
for x in column_titles:
    old_new_titles[x] = column_titles[x][0]

sframe.rename(old_new_titles)
data_frame = sframe.to_dataframe()
data_frame = data_frame[1:]
sframe = gl.load_sframe(data_frame)
sframe.save('dataset')

sframe = gl.load_sframe('dataset')
print('------after renamning columns and removing the first row (data stored back to the disk):------')
print(sframe.head(5))
