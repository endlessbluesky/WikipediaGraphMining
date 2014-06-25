#---need to activate virtualenv to import MySQLdb---
#shell command:
#source graphlab/bin/activate

import graphlab
import MySQLdb as mysql
import stop_watch

#create a database connection object
#type(db) = MySQLdb.connections.Connection
db = mysql.connect(host = 'localhost',
                   user = 'gaoyuan',
                   passwd = 'gaoyuan',
                   db = 'gaoyuan_wikipedia')

#get a cursor of the selected table 'page' (basic info of all pages)
page_info_sframe = graphlab.SFrame()
cur = db.cursor()
stop_watch.reset()
stop_watch.start()
list_of_pages = []
cur.execute('SELECT * FROM page LIMIT 100')


#close cursor and close database connection
cur.close()
db.close()
stop_watch.stop()
print('time cost for loading sql data = ' + str(stop_watch.getTimeElapsedInSeconds()))

