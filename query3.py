#-*- encoding: utf-8 -*-
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from random import randint
from datetime import date
from bson.son import SON
from datetime import datetime, timedelta
from pprint import pprint
import sys
import random

client = MongoClient('mongodb://localhost:27017/')
db = client['test']

def delete():
	db.drop_collection("query3")

def insertsQ3():
	delete()




	insertsQuery3 = [
		{
		"_id" : 1,
		"orderKey": 1,
		"orderDate": 1,
		"mktSegment": 4,
		"shippriority": 1,
		"lineItems": [{
		  "discount": 0.5,
		  "extendedPrice": 20,
		  "shipDate": 19,
		  "returnFlag": 'a',
		  "quantity": 1,
		  "lineStatus": 2,
		  "tax": 0.1
		},{
		  "discount": 0.3,
		  "extendedPrice": 20,
		  "shipDate"      : 23,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 2
		}
		],
	"regionName" : 'Catalunya',
	"nationName" : 'Francia'
	},
	{
		"_id" : 2,
		"orderKey"   : 2,
		"orderDate"  : 3,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.6,
		  "extendedPrice" : 2,
		  "shipDate"      : 7,
		  "returnFlag"    : 'a',
		  "quantity"      : 2,
		  "lineStatus"    : 2,
		  "tax"           : 0.9
		},{
		  "discount"      : 0.1,
		  "extendedPrice" : 9,
		  "shipDate"      : 16,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Aragon',
	"nationName" : 'Portugal'
	},
	{
		"_id" : 3,
		"orderKey"   : 3,
		"orderDate"  : 7,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.4,
		  "extendedPrice" : 35,
		  "shipDate"      : 7,
		  "returnFlag"    : 'a',
		  "quantity"      : 2,
		  "lineStatus"    : 2,
		  "tax"           : 0.9
		},{
		  "discount"      : 0.3,
		  "extendedPrice" : 2,
		  "shipDate"      : 16,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Aragon',
	"nationName" : 'Portugal'
	},
	{
		"_id" : 4,
		"orderKey"   : 4,
		"orderDate"  : 3,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.4,
		  "extendedPrice" : 35,
		  "shipDate"      : 7,
		  "returnFlag"    : 'a',
		  "quantity"      : 2,
		  "lineStatus"    : 2,
		  "tax"           : 0.9
		},{
		  "discount"      : 0.3,
		  "extendedPrice" : 2,
		  "shipDate"      : 4,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Aragon',
	"nationName" : 'Portugal'
	},
	{
		"_id" : 5,
		"orderKey"   : 5,
		"orderDate"  : 2,
		"mktSegment" : 6,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.4,
		  "extendedPrice" : 35,
		  "shipDate"      : 17,
		  "returnFlag"    : 'a',
		  "quantity"      : 2,
		  "lineStatus"    : 2,
		  "tax"           : 0.9
		},{
		  "discount"      : 0.3,
		  "extendedPrice" : 2,
		  "shipDate"      : 26,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Aragon',
	"nationName" : 'Portugal'
	},
	]
	db.query3.insert(insertsQuery3);



print() #linea en blanco
insertsQ3()


segment = 4
date1 = 5
date2 = 15
l = db.query3.aggregate([
	#{"$unwind": "$orders"},
	{"$unwind": "$lineItems"},
	#{"$unwind": "$orders.lineitems.quantity"},
	{"$match": {
		"mktSegment": segment,
		"orderDate": {"$lt": date1},
		"lineItems.shipDate": {"$gt": date2}
	}}
	,
	{"$group": {
		"_id": {"orderKey": "$orderKey","orderDate": "$orderDate", "shippriority": "$shippriority"},
		"revenue": {"$sum": {"$multiply": [{"$subtract": [1,"$lineItems.discount"]}, "$lineItems.extendedPrice"]}}
	}},
	{"$sort": {
		"revenue": -1, "orderdate": 1
	}}

])

print(list(l))

#print(list(db.query3.find()))
