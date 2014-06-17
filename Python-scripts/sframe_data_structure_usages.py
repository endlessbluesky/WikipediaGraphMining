import graphlab as gl 
import numpy as np
import pandas

data_frame = pandas.DataFrame()
sframe = gl.SFrame(data = data_frame)
sframe = gl.SFrame.read_csv('http://testdatasets.s3-website-us-west-2.amazonaws.com/users.csv.gz',
                         delimiter=',',
                         header=False,
                         comment_char="#",
                         column_type_hints={'user_id': int})

sframe.save('test_data_set_csv')
