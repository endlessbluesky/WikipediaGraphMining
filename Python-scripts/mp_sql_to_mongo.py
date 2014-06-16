import pymongo
import MySQLdb as mysql
from multiprocessing import Process, Queue

db = mysql.connect(host = 'localhost', user = 'gaoyuan', passwd = 'gaoyuan', db = 'gaoyuan_wikipedia')
cur = db.cursor()
#     cur.execute('SELECT * FROM pagelinks LIMIT 1000')
client = pymongo.MongoClient('localhost', 27017)
pl = client['wiki']['page_links']
newpl = client['wiki']['page_to_page_link_records']

def getIdFromTitle(title):
    #due to efficiency concern there is no error handing
    #but probably if would raise error if pl.find_one(**) returns None
    #then NoneType['something'] throws the error
    return pl.find_one({'title': title})['_id']

def formatTitle(title):
    return title.replace('_', ' ')

################################
#one single record look something like this:
#(12L, 0L, 'Anarchism_in_Cuba'), where
#12 is the '_id' of the page, 'Anarchism_in_Cuba' is the title of the target of the link
#the input entry is a tuple similar as above
#for speed requirement, no duplicate check, no validity check, no nothing
################################

#deal with one '_id', i.e., one page and all its links
#parallelizable since inserting one entry is set to be 'atomic'
def processId(id):
    cur = db.cursor()
    cur.execute('SELECT * FROM pagelinks WHERE pl_from = ' + str(id))
    #must first complete entry construction then insert it into the database
    entry = {'_id': id, 'links': []}
    for row in cur: #row = (_id, _namespace, title)
        entry['links'].append(getIdFromTitle(formatTitle(row[2])))
    newpl.insert(entry)

def processFromTo(begin, end):
    mongocur = pl.skip(begin-1)
    for i in range(begin, end+1):
        processId(mongocur.next()['_id'])
    print("done from" + str(begin) + " to " + str(end))

if __name__ == '__main__':
    print('started transfer data from mysql to mongo...')
    total = 14313024
    step = int(14313024/128) + 1
    processes = list(range(1, total, step))
    processes = list(map(lambda begin: (begin, min(begin + step - 1, total)), processes))
#   p = Process(target=f, args=(q,))    
    processes = list(map(lambda beginEndTuple: Process(target = processFromTo, args = beginEndTuple), processes))
    print('can start already!')
#     plcursor = pl.find().limit(10000000)
#     for entry in plcursor:
#         if cur.execute('SELECT EXISTS(SELECT 1 FROM pagelinks WHERE pl_from = ' +str(entry['_id']) + ')') == 0:
#             print('_id ' + str(entry['_id']) + ' does not exist in mysql database!')    
#     print('all _ids exist in mysql database!')

    