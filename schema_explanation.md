### What schema do we use?

We will use the following document for the queries 1, 3 and 4:
```json
{
	orderKey   : ObjectId,
	orderDate  : Date,
	mktSegment : String,
	shipPriority : Number,
	lineItems  : [{
      discount      : Number,
      extendedPrice : Number,
      shipDate      : Date,
      returnFlag    : String,
      quantity      : Number,  
      lineStatus    : Number,
      tax           : Number
	}],
	regionName : String,
	nationName : String
}
```

the following one for the query 2:
```json
{
	suppKey    : ObjectId,
	name       : String,
	address    : String,
	phone      : String,
	acctbal    : String,
	comment    : String,
	nationName : String,
	regionName : String,
	parts      : [{
	     partKey     : String,
       mfgr        : String,
       type        : String,
       size        : Number,
       supplyCost  : Number
	}]
}
```
