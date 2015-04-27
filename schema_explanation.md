### What schema do we use?

We will use the following document for the query 1:
```json
{
	shippdate      : ObjectId,
	returnFlag     : String,
	quantity       : Number,
	extendedPrice  : Number,
	discount       : Number,
  tax            : Number
}
```

the following one for the query 2:
```json
{
	suppKey   : ObjectId,
	name      : String,
	address   : String,
	phone     : String,
	acctbal 	: String,
	comment 	: String,
	nationName: String,
	regionName: String,
	parts 		: [{
		partKey 	: String,
		mfgr 		  : String,
		type		  : String,
		size		  : Number,
		supplyCost: Number
	}]
}
```

and the following ones for the queries 3 and 4:
```json
{
	orderKey	 : ObjectId,
	orderDate	 : Date,
	mktSegment : String,
	lineItems  : [{
		discount		  : Number,
    extendedPrice : Number,
		shipDate		  : Date
	}],
	regionName : String,
	nationName : String
}
```
