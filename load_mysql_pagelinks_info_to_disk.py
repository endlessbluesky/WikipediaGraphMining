#import graphlab
import MySQLdb
import stop_watch

#create a database connection object
#type(db) = MySQLdbdb.connections.Connection
db = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')

#set the cursor type to SSCursor so that
#the result would be indices for reference only
#and would be stored on the server side
#otherwise a single execution would load
#all rows into memory
db.cursorclass = MySQLdb.cursors.SSCursor
#get a dummy cursor
cur_pagelinks = db.cursor()
#ensure that the type is indeed the desired SSCursor
assert(str(type(cur_pagelinks)) == ("<class 'MySQLdb.cursors.SSCursor'>"))
cur_pagelinks.execute('SELECT * FROM pagelinks')
stop_watch.stop()
print('number of rows fetched = ' + str(cur_pagelinks.rowcount))

#reset and start the stop watch
stop_watch.reset()
stop_watch.start()

#the main iteration
count = 0
for row in cur_pagelinks:
    if(row[1] != 0):
        continue
    count += 1
    if(count == 2000000):
        print(count)
        break
stop_watch.stop()
print(stop_watch.getTimeElapsedInSeconds())
    
#wiki_graph = graphlab.SGraph()
        

################################
#in the table pagelinks,
#one single record (row, pagelink, etc.) look something like this:
#(12L, 0L, 'Anarchism_in_Cuba'), where
#12 is the '_id' of the page, 'Anarchism_in_Cuba' is the
#title of the target page of the link
################################

