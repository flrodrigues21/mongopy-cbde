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
	db.drop_collection("query1")

def insertsQ1():
	delete()

	insertsQuery1 = [
		{
		"_id" : 1,
		"orderKey": 1,
		"orderDate": 15,
		"mktSegment": 4,
		"shippriority": 1,
		"lineItems": [{
		  "discount": 0.9,
		  "extendedPrice": 5,
		  "shipDate": 15,
		  "returnFlag": 'a',
		  "quantity": 4,
		  "lineStatus": 2,
		  "tax": 0.1
		},{
		  "discount": 0.4,
		  "extendedPrice": 32,
		  "shipDate"      : 5,
		  "returnFlag"    : 'a',
		  "quantity"      : 1,
		  "lineStatus"    : 2,
		  "tax"           : 0.4
		}
		],
	"regionName" : 'Catalunya',
	"nationName" : 'Francia'
	},
	{
		"_id" : 2,
		"orderKey"   : 2,
		"orderDate"  : 15,
		"mktSegment" : 4,
		"shippriority": 1,
		"lineItems"  : [{
		  "discount"      : 0.1,
		  "extendedPrice" : 3,
		  "shipDate"      : 20,
		  "returnFlag"    : 'a',
		  "quantity"      : 2,
		  "lineStatus"    : 2,
		  "tax"           : 0.9
		},{
		  "discount"      : 0.5,
		  "extendedPrice" : 20,
		  "shipDate"      : 3,
		  "returnFlag"    : 'a',
		  "quantity"      : 5,
		  "lineStatus"    : 2,
		  "tax"           : 0.05
		}
		],
	"regionName" : 'Portugal',
	"nationName" : 'Aragon'
	}
	]
	db.query1.insert(insertsQuery1);



print() #linea en blanco
insertsQ1()

date = 15
l = db.query1.aggregate([
	{"$unwind": "$lineItems"},
	{"$match": {
		"lineItems.shipDate": {"$lte": date}
	}},
	{"$group": {
		"_id": {"returnFlag": "$lineItems.returnFlag","lineStatus": "$lineItems.lineStatus"},
		"sum_qty": {"$sum": "$lineItems.quantity"},
		"sum_base_price": {"$sum": "$lineItems.extendedPrice"},
		"sum_disc_price": {"$sum": {"$multiply": [{"$subtract": [1,"$lineItems.discount"]}, "$lineItems.extendedPrice" ]}},
		"sum_charge": {"$sum": {"$multiply": [{"$subtract": [1,"$lineItems.discount"]}, {"$add": [1,"$lineItems.tax"]}, "$lineItems.extendedPrice"]}},
		"avg_qty": {"$avg": "$lineItems.quantity"},
		"avg_price": {"$avg": "$lineItems.extendedPrice"},
		"avg_disc": {"$avg": "$lineItems.discount"},
		"count_order": {"$sum": 1}
	}},
	{"$sort": {
		"returnFlag": 1, "lineStatus": 1
	}}
])

print(list(l))

#print(list(db.query1.find()))
