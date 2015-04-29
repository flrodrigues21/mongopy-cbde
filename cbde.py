"""
To start MongoDB:
mongod
To Insert Data from a file:
mongoimport --db databaseName --collection collectionName --drop --file file.json
Example:
mongoimport --db test --collection restaurants --drop --file primer-dataset.json

To Delete DB:
mongo cbde --eval "db.dropDatabase()"
"""

# This function inserts two documents with different values
def insertData():
    db.queryone
    db.queryone.insert(
        [{
            "_id":  1,
        	"orderKey"   : ObjectId,
        	"orderDate"  : Date,
        	"mktSegment" : String,
            "shipPriority" : Number,
        	"lineItems"  : [{
              "discount"      : Number,
              "extendedPrice" : Number,
              "shipDate"      : Date,
              "returnFlag"    : String,
              "quantity"      : Number,
              "lineStatus"    : Number,
              "tax"           : Number
        	}],
        	"regionName" : String,
        	"nationName" : String
        }
        ]
    )
    return;


# This function searchs the documents that fits the criteria inside the find
# In our example, it will display the first document
def getData():
    result = db.queryone.find({"quantity": 1})
    return result;

from pymongo import MongoClient
client = MongoClient()
db = client.cbde              #Connection to Database
insertData()
result = getData()

for document in result:
    print(document)
