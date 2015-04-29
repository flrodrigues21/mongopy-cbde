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
	db.drop_collection("query4")

def insertsQ4():
	delete()


	insertsQuery4 = [
		{
		"_id" : 1,
		"orderKey": 1,
		"orderDate": 25,
		"mktSegment": 4,
		"shippriority": 1,
		"lineItems": [{
		  "discount": 0.1,
		  "extendedPrice": 20,
		  "shipDate": 10,
		  "returnFlag": 'a',
		  "quantity": 1,
		  "lineStatus": 2,
		  "tax": 0.9
		},{
		  "discount": 0.2,
		  "extendedPrice": 2,
		  "shipDate"      : 23,
		  "returnFlag"    : 'a',
		  "quantity"      : 2,
		  "lineStatus"    : 2,
		  "tax"           : 0.5
		}
		],
	"regionName" : 'Catalunya',
	"nationName" : 'Espanya'
	},
	{
		"_id" : 2,
		"orderKey"   : 2,
		"orderDate"  : 47,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.2,
		  "extendedPrice" : 5,
		  "shipDate"      : 10,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.2
		},{
		  "discount"      : 0.8,
		  "extendedPrice" : 6,
		  "shipDate"      : 15,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Catalunya',
	"nationName" : 'Francia'
	},
	{
		"_id" : 3,
		"orderKey"   : 3,
		"orderDate"  : 122,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.2,
		  "extendedPrice" : 5,
		  "shipDate"      : 10,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.2
		},{
		  "discount"      : 0.8,
		  "extendedPrice" : 6,
		  "shipDate"      : 15,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Aragon',
	"nationName" : 'Francia'
	},
	{
		"_id" : 4,
		"orderKey"   : 4,
		"orderDate"  : 15,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.2,
		  "extendedPrice" : 5,
		  "shipDate"      : 10,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.2
		},{
		  "discount"      : 0.8,
		  "extendedPrice" : 6,
		  "shipDate"      : 15,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Catalunya',
	"nationName" : 'Francia2'
	},
	{
		"_id" : 5,
		"orderKey"   : 5,
		"orderDate"  : 384,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.2,
		  "extendedPrice" : 5,
		  "shipDate"      : 10,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.2
		},{
		  "discount"      : 0.8,
		  "extendedPrice" : 6,
		  "shipDate"      : 15,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.3
		}
		],
	"regionName" : 'Catalunya',
	"nationName" : 'Portugal'
	}
	]
	db.query4.insert(insertsQuery4);



print() #linea en blanco
insertsQ4()


region = 'Catalunya'
date1 = 20
date2 = 385
l = db.query4.aggregate([

	{"$unwind": "$lineItems"},

	{"$match": {
		"orderDate": {"$gte": date1, "$lt" : date2},
		#"orderDate": {"$lt": date2},
		"regionName": region

	}}
	,
	{"$group": {
		"_id": {"nationName": "$nationName"},
		"revenue": {"$sum": {"$multiply": [{"$subtract": [1,"$lineItems.discount"]}, "$lineItems.extendedPrice"]}}
	}},
	{"$sort": {
		"revenue": -1
	}}

])

print(list(l))

#print(list(db.query2.find()))
