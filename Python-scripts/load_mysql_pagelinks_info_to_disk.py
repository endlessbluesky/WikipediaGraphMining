#import graphlab
import MySQLdb
import stop_watch

#create a database connection object
#type(db) = MySQLdbdb.connections.Connection
db = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')

db_copy = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')

#set the cursor type to SSCursor so that
#the result would be indices for reference only
#and would be stored on the server side
#otherwise a single execution would load
#all rows into memory
db.cursorclass = MySQLdb.cursors.SSCursor
db_copy.cursorclass = MySQLdb.cursors.SSCursor
#get a dummy cursor
cur_pagelinks = db.cursor()
cur_page_info = db_copy.cursor()

#ensure that the type is indeed the desired SSCursor
assert(str(type(cur_pagelinks)) == ("<class 'MySQLdb.cursors.SSCursor'>"))
assert(str(type(cur_page_info)) == ("<class 'MySQLdb.cursors.SSCursor'>"))

cur_pagelinks.execute('SELECT * FROM pagelinks')
#cur_page_info.execute("SELECT * FROM page LIMIT 1")
print('number of rows fetched = ')
print(cur_pagelinks.rowcount)

#reset and start the stop watch
stop_watch.reset()
stop_watch.start()

#the main iteration
#pick one row from the pagelinks table
#determine the id of the target page

print(cur_pagelinks.next())
for i in range(20):
    cur_page_info = db_copy.cursor()
    row = cur_pagelinks.next()
    if(row == None):
        break
    if(row[1] != 0):
        continue
    cur_page_info.execute("SELECT * FROM page WHERE page_title = '" + row[2] + "'" + "LIMIT 1")
    corresponding_page = cur_page_info.fetchone()
    if(corresponding_page == None):
        print('error! non-existing page-title!')

stop_watch.stop()
print('time to iterate through the rows = ' + str(stop_watch.getTimeElapsedInSeconds()))
print('closing the database connections')
db.close()
db_copy.close()
print('------done all------')

#wiki_graph = graphlab.SGraph()
        

################################
#in the table pagelinks,
#one single record (row, pagelink, etc.) look something like this:
#(12L, 0L, 'Anarchism_in_Cuba'), where
#12 is the '_id' of the page, 'Anarchism_in_Cuba' is the
#title of the target page of the link
################################

