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
comprehensive_page_info = client['wiki']['comprehensive_page_info']

#one row contains:
list_of_fields = ['page_id', 'page_namespace', 'page_title',
                  'page_restrictions', 'page_conuter', 'page_isredirect',
                  'page_is_new', 'page_random', 'page_touched',
                  'page_links_updated', 'page_latest', 'page_len']

def transfer_one_page(page_id):
    #definition of local variable would shield the global ones
    db = MySQLdb.connect(host = 'localhost',
                     user = 'gaoyuan',
                     passwd = 'gaoyuan',
                     db = 'gaoyuan_wikipedia')
    db.cursorclass = MySQLdb.cursors.SSCursor
    cur_page_info = db.cursor()
    cur_page_info.execute('SELECT * FROM page where page_id= ' + str(page_id))
    entry = cur_page_info.fetchone()
    if(entry == None):
        return
    mongo_page_dict = dict(zip(list_of_fields, entry))
    comprehensive_page_info.insert(mongo_page_dict)

def from_to(begin, end):
    for page_id in range(begin, end+1):
        transfer_one_page(page_id)
    print('done with the interval ' + str((begin, end)))

def dummy(begin, end):
    print("begin = " + str(begin) + ", end = " + str(end))

if __name__ == '__main__':
    print('in __main__!')
##    process_1 = Process(target = dummy, args = (1,2))
##    process_2 = Process(target = dummy, args = (3,4))
##    process_1.start()
##    process_2.start()
##
##    process_1.join()
##    process_2.join()
    
    print('started transfer data from mysql to mongo')
    MAX_ID = 42644405 # slightly greater than the largest id
    step = int(42644405/16) + 1
    processes = list(range(0, MAX_ID, step))
    processes = list(
        map(lambda begin: (begin, min(begin + step - 1, MAX_ID)),
            processes))
    print(processes)
    print('total number of processes = ' + str(len(processes)))
    processes = list(map(lambda beginEndTuple: Process(target = from_to, args = beginEndTuple), processes))
    print('started!')
    map(lambda x: x.start(), processes)
    map(lambda x: x.join(), processes)

##unused methods mainly for reference
##stop_watch.reset()
##stop_watch.start()
##count = 0
##for entry in cur_page_info:
##    count += 1
##    mongo_page_dict = dict(zip(list_of_fields, entry))
##    comprehensive_page_info.insert(mongo_page_dict)
##    if count == 1000:
##        break
##
##stop_watch.stop()
##time_elapsed = stop_watch.getTimeElapsedInSeconds()
##print('time_elapsed = ' + str(time_elapsed))
