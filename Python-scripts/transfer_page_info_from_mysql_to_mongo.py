import pymongo
import MySQLdb
from multiprocessing import Process
import stop_watch

#create a mysql connection
db = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')

#set the cursor class to be "server-side loading" mode
db.cursorclass = MySQLdb.cursors.SSCursor
#get a cursor
#cur_page_info = db.cursor()
#make sure the cursor is of the correct type
#assert(str(type(cur_page_info)) == ("<class 'MySQLdb.cursors.SSCursor'>"))
#execute the dummy query
#cur_page_info.execute('SELECT * FROM page')
#create a mongodb connection
client = pymongo.MongoClient('localhost', 27017)
#create a new collection
comprehensive_page_info = client['wiki']['comprehensive_page_info_reproduced']

#one row contains:
list_of_fields = ['page_id', 'page_namespace', 'page_title',
                  'page_restrictions', 'page_conuter', 'page_isredirect',
                  'page_is_new', 'page_random', 'page_touched',
                  'page_links_updated', 'page_latest', 'page_len']

db.cursorclass = MySQLdb.cursors.SSCursor
cur_page_info = db.cursor()
cur_page_info.execute('SELECT * FROM page')

count= 0
for entry in cur_page_info:
    mongo_dict_entry = dict(zip(list_of_fields, entry))
    count += 1
    comprehensive_page_info.insert(mongo_dict_entry)

print('done!')
        
