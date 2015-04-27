/*
To start MongoDB:
mongod
To Insert Data from a file:
mongoimport --db databaseName --collection collectionName --drop --file file.json
Example:
mongoimport --db test --collection restaurants --drop --file primer-dataset.json
*/

from pymongo import MongoClient
client = MongoClient()
db = client.test
cursor = db.restaurants.find({"borough": "Manhattan"})

for document in cursor:
    print(document)
