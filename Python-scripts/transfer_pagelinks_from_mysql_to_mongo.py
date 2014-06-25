import MySQLdb
import pymongo

#create a mysql connection
db_1 = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')

#set the cursor class to be "server-side loading" mode
db_1.cursorclass = MySQLdb.cursors.SSCursor
#get a cursor
cur_pagelinks_1 = db_1.cursor()
#make sure the cursor is of the correct type
assert(str(type(cur_pagelinks_1)) == ("<class 'MySQLdb.cursors.SSCursor'>"))
#execute the dummy query
cur_pagelinks_1.execute('SELECT * FROM pagelinks LIMIT 1 OFFSET 1')

#repeat for db_2 and cur_pagelinks_2
db_2 = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')
db_2.cursorclass = MySQLdb.cursors.SSCursor
cur_pagelinks_2 = db_2.cursor()
assert(str(type(cur_pagelinks_2)) == ("<class 'MySQLdb.cursors.SSCursor'>"))
cur_pagelinks_2.execute('SELECT * FROM pagelinks')

#create mysql page_info_cursor
db_3 = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')
db_3.cursorclass = MySQLdb.cursors.SSCursor
mysql_page_info_cursor = db_3.cursor()
assert(str(type(mysql_page_info_cursor)) == ("<class 'MySQLdb.cursors.SSCursor'>"))
#closing this cursor costs very long time
#therefore close the database connection instead
mysql_page_info_cursor.execute('SELECT * FROM page')

counter = 0
for page in mysql_page_info_cursor:
    
    if(page[1]==14):
        print(page)
    if(counter == 1000000):
        break

print('done with the loop')

#create a mongodb connection
client = pymongo.MongoClient('localhost', 27017)
#notice that we only consider articles in namespace 0, i.e., main entries/articles
page_links_graph = client['wiki']['page_links_graph_main_articles']


counter = 0


current = None
frontier = None

cur_pagelinks_1.close()
cur_pagelinks_1 = db_1.cursor()
cur_pagelinks_1.execute('SELECT * FROM pagelinks LIMIT 1 OFFSET 10000')

db_1.close()
db_2.close()
db_3.close()
