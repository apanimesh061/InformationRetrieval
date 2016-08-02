# -------------------------------------------------------------------------------
# Name:        mergeDBs
# Purpose:     merge DBs from different systems
#
# Author:      Animesh Pandey
#
# Created:     29/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from collections import Counter

client = MongoClient('mongodb://localhost:27017/')
db2 = client['bu_wiki2']
db = client['wiki_data2']
new_db = client['crawl2']
ol_table = db['outlink_data']
il_table = db['inlink_data']
map_table = db['url_hash']

map_table2 = new_db['url_hash']
ol_table2 = new_db['outlink_data']
il_table2 = new_db['inlink_data']


##'011920be72c7dbe868e1ada65bd02eeb'

##a = il_table.find_one({'_id' : '011920be72c7dbe868e1ada65bd02eeb'})
##b = il_table2.find_one({'_id' : '011920be72c7dbe868e1ada65bd02eeb'})

##for datum in il_table.find():
##    db2['inlink_data'].insert(datum)

def merge_inlinks(val1, val2):
    temp_dict1 = dict()
    temp_dict2 = dict()
    for d in val1['inlinks']: temp_dict1.update(d)
    for d in val2['inlinks']: temp_dict2.update(d)

    new_val = dict()
    new_val["_id"] = val1['_id']
    new_val['inlinks'] = \
        [{i[0]: i[1]} for i in dict(Counter(temp_dict1) + Counter(temp_dict2)).items()]
    new_val['inlink_count'] = len(new_val['inlinks'])

    return new_val


##for val in il_table.find():
##    try:
##        map_table.insert(val)
##    except DuplicateKeyError, e:
##        print e

##for val in il_table2.find():
##    try:
##        il_table.insert(val)
##    except DuplicateKeyError, e:
##        new_val = merge_inlinks(val, il_table2.find_one({'_id' : val['_id']}))
##        il_table.update({'_id': val['_id']}, new_val, upsert=True)

for val in il_table.find():
    dat = il_table2.find_one({'_id': val['_id']})
    if dat:
        curr_il = dat['inlinks']
        try:
            new_curr_il = [{i[0]: i[1]} for i in curr_il.items()]
            il_table.update({"_id": val['_id']}, {"$set": {'inlinks': new_curr_il}})
            print curr_il, 'to', new_curr_il
        except:
            pass
            ##for val in il_table2.find():
            ##    ol_table.insert(val)
            ##
            ##for val in ol_table2.find():
            ##    ol_table.insert(val)
