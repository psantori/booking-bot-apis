'use strict';

require('dotenv').config()
var filter = require('filter-values');

// Db related methods
function initDb(callback) {
    getOrCreateDatabase(databaseId, function (db) {
        getOrCreateCollection(db._self, collectionId, function (coll) {
            callback();
        });
    });
}

function createDocument(collLink, documentDefinition, cb) {
    client.createDocument(collLink, documentDefinition, function (err, document) {
        if (err) {
            console.log(err);
        } else {
            console.log('created ' + document.id);
            cb(document);
        }
    });
}

function deleteDocument(docLink, cb) {
    client.deleteDocument(docLink, function (err) {
        if (err) {
            handleError(err);
        } else {
            console.log('Document deleted');
            cb();
        }
    });    
}

function queryDocuments(collLink, querySpec, cb) {
    const options = {};
    client.queryDocuments(collLink, querySpec, options).toArray(function (err, results) {
        if (err) {
            console.log(err);
            console.log(JSON.parse(err).message);
            if (new String(err.code).charAt(0) === '4') {
                cb(err, undefined);
            } else {
                handleError(err);
            }
        }
        cb(undefined, results);
    });
}

function readDocument(docLink, callback) {
    client.readDocument(docLink, function (err, doc, headers) {
        if (err) {
            if (new String(err.code).charAt(0) === '4') {
                callback(err, undefined);
            } else {
                handleError(err);
            }
        } else {
            console.log('Document \'' + docLink + '\' found');
            callback(undefined, doc);
        }
    });
}

function getOrCreateCollection(dbLink, id, callback) {
    //we're using queryCollections here and not readCollection
    //readCollection will throw an exception if resource is not found
    //queryCollections will not, it will return empty resultset. 
    
    //the collection we create here just uses default IndexPolicy, default OfferType. 
    //for more on IndexPolicy refer to the IndexManagement samples
    //for more on OfferTye refer to CollectionManagement samples

    var querySpec = {
        query: 'SELECT * FROM root r WHERE r.id=@id',
        parameters: [
            {
                name: '@id',
                value: id
            }
        ]
    };
    
    client.queryCollections(dbLink, querySpec).toArray(function (err, results) {
        if (err) {
            handleError(err);
            
        //collection not found, create it
        } else if (results.length === 0) {
            var collDef = { id: id };
            
            client.createCollection(dbLink, collDef, function (err, created) {
                if (err) {
                    handleError(err);
                
                } else {                    
                    callback(created);
                }
            });
        
        //collection found, return it
        } else {
            callback(results[0]);
        }
    });
}

function getOrCreateDatabase(id, callback) {
    //we're using queryDatabases here and not readDatabase
    //readDatabase will throw an exception if resource is not found
    //queryDatabases will not, it will return empty resultset. 
    
    var querySpec = {
        query: 'SELECT * FROM root r WHERE r.id=@id',
        parameters: [
            {
                name: '@id',
                value: id
            }
        ]
    };
    
    client.queryDatabases(querySpec).toArray(function (err, results) {
        if (err) {
            handleError(err);

        //database not found, create it
        } else if (results.length === 0) {
            var databaseDef = { id: id };
            
            client.createDatabase(databaseDef, function (err, created) {
                if (err) {
                    handleError(err);
                
                } else {                    
                    callback(created);
                }
            });
        
        //database found, return it
        } else {
            callback(results[0]);
        }
    });
}

function handleError(error) {
    console.log('\nAn error with code \'' + error.code + '\' has occurred:');
    console.log('\t' + JSON.parse(error.body).message);
}

// Db connection
var DocumentClient = require('documentdb').DocumentClient;
const host = process.env.DOCUMENTDB_HOST;
const masterKey = process.env.DOCUMENTDB_AUTHKEY;
const databaseId = process.env.DOCUMENTDB_DB_ID;
const collectionId = process.env.DOCUMENTDB_COLLECTION_ID;
const client = new DocumentClient(host, {masterKey: masterKey});

// Db instances
let dbLink;
let collLink;
initDb(function (err) {
    if (!err) {
        dbLink = 'dbs/' + databaseId;
        console.log(dbLink);
        collLink = dbLink + '/colls/' + collectionId;
        console.log(collLink);
    }
});

// Create a server with a host and port
const express = require('express');
const bodyParser = require('body-parser');
const app = express();

// Load middleware
app.use(bodyParser.json());

// Validators
const { check, validationResult } = require('express-validator/check');
const { matchedData, sanitize } = require('express-validator/filter');

// Config
const safeBookingFields = ['id', 'userId', 'storeId', 'agentId', 'date'];
const searchableBookingFields = ['userId', 'storeId', 'agentId', 'date'];

app.post('/bookings',
    check('userId')
        .exists().withMessage('Gimme a userId'),
    check('storeId')
        .exists().withMessage('Gimme a storeId'),
    check('agentId')
        .exists().withMessage('Gimme a agentId'),
    check('date')
        .exists().withMessage('Gimme a date')
        .isISO8601().withMessage('Gimme a valid ISO8601 date')
 , (req, res, next) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(422).json({ errors: errors.mapped() });
    }
    const booking = matchedData(req);
    createDocument(collLink, booking, function (doc) {
        const result = filter(doc, function(value, key, obj) {
            return safeBookingFields.indexOf(key) != -1;
        });
        res.json(result);
    })
});

app.delete('/bookings/:bookingId', function (req, res) {
    console.log("Deleting ID: " + req.params.bookingId);
    var docLink = collLink + '/docs/' + req.params.bookingId;
    let doc = deleteDocument(docLink, function(doc) {
        res.json({
            id: req.params.bookingId
        });
    });
})

app.get('/bookings/:bookingId', function (req, res) {
    console.log("ID: " + req.params.bookingId);
    var docLink = collLink + '/docs/' + req.params.bookingId;
    let doc = readDocument(docLink, function(err, doc) {
        if (err) {
            return res.status(err.code).send({
                id: req.params.bookingId,
                error: "Not found"
            });
        }
        const result = filter(doc, function(value, key, obj) {
            return safeBookingFields.indexOf(key) != -1;
        });
        res.json(result);
    });
})

app.get('/bookings', function (req, res) {
    let querySpec, queryWhere = [];
    const collectionAlias = 'b';
    const parameters = searchableBookingFields.reduce(function(acc, cur, i) {
        if (req.query[cur]) {
            queryWhere.push(collectionAlias + '.' + cur + ' = @' + cur);
            acc.push({
                name: '@' + cur,
                value: req.query[cur]
            })
        }
        return acc;
    }, []);
    querySpec = {
        query: "SELECT * FROM Bookings " + collectionAlias + (queryWhere.length ? " WHERE " + queryWhere.join(" AND ") : ""),
        parameters: parameters
    }
    queryDocuments(collLink, querySpec, function (err, results) {
        if (err) {
            return res.status(err.code).send({
                query: req.query,
                error: "Not found"
            });
        }
        if (results.length === 0) {
            res.send({});
        }
        let documents = results.map((val, index, arr) => {
            return filter(val, function(v, key, o) {
                return safeBookingFields.indexOf(key) != -1;
            });
        });
        res.send(documents);
    })
})

app.listen(process.env.HOST_PORT, function () {
    console.log('Example app listening on port ' + process.env.HOST_PORT);
});
  