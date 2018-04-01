'use strict';

var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
var docClient = new AWS.DynamoDB.DocumentClient();
var s3 = new AWS.S3();
var AWSregion = 'us-east-1';  //N. Virginia
var SNS = new AWS.SNS();
var s3params = {Bucket: 'newcomer-isengard-training', Key: process.env.csvFile};
AWS.config.update({region: AWSregion});

var tableName = process.env.TableName;
var subjectName;
var columnNames = [];
var flashList = [];
var i = 0;
var j = 0;

const mobileNum = '+17606174123';  // Alan's Cell
//const mobileNum = '+14802085948';  // Robert's Cell

const emoji = {
    'thumbsup':     '\uD83D\uDC4D',
    'smile':        '\uD83D\uDE0A',
    'star':         '\uD83C\uDF1F',
    'robot':        '\uD83E\uDD16',
    'germany':      '\ud83c\udde9\ud83c\uddea',
    'uk':           '\ud83c\uddec\ud83c\udde7',
    'usa':          '\ud83c\uddfa\ud83c\uddf8'
}

const bodyText = 'Hello! ' + emoji.smile + ' \n'
        + emoji.germany + emoji.uk + emoji.usa + '\n'
    + 'Here is your link: \n'
    + 'https://www.youtube.com/watch?v=2BIWNLC_iT4';


var phoneParams = {
    PhoneNumber: mobileNum,
    Message: "Adding fields to DataBase"};
    
//phoneParams.Message = "Changed Message";

function sendTxtMessage(phoneParams, callback) {
    SNS.publish(phoneParams, function(err, data){
        console.log('sending message to ' + phoneParams.PhoneNumber.toString() );
        if (err) {
            console.log(err, err.stack);
        }
        callback(null, console.log('Text message sent to: ' + mobileNum));
    });
}

function checkTable(lTableName, callback) {
    console.log("Check table: " + lTableName);
    var params = {
        TableName: lTableName /* required */
    };
    dynamodb.describeTable(params, function(err, data) {
        if (err) {
            //console.log(err, err.stack); // an error occurred
            console.log('No Table');
            callback(createTable(lTableName, callback))
        }
        else {
            console.log('Table exists'); // successful response
            phoneParams.Message = "Table Aready Exists";
            //callback(sendTxtMessage(phoneParams, callback));
            callback(readCSV(0,0,callback));
            //callback();
        }
    });
    callback(console.log("Text commented out"));
}

function createTable(lTableName, callback) {
    
    //tableName = data[0];
    //this.tableName = tableName;
    var dbparams = {
        TableName : lTableName,
        KeySchema: [       
            { AttributeName: "Subject", KeyType: "HASH"},  //Partition key
                //{ AttributeName: "title", KeyType: "RANGE" }  //Sort key
                ],
                AttributeDefinitions: [       
                { AttributeName: "Subject", AttributeType: "S" },
                //{ AttributeName: "title", AttributeType: "S" }
                ],
                ProvisionedThroughput: {       
                ReadCapacityUnits: 10, 
                WriteCapacityUnits: 10
            }
        };
        
    dynamodb.createTable(dbparams, function(err, data) {
        if (err) {
        console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
        callback(console.log("No Table Created Due to an Error"));
        } else {
        //console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
        console.log("Created Table");
        //callback(console.log("Table Created"));
        
        const tableparams = {
        TableName: lTableName
        };
        
        dynamodb.waitFor('tableExists', tableparams, (err, data) => {
            if (err) {
                console.log(err, err.stack);
            } else  {
                console.log('Table Created.');
                phoneParams.Message = "Table Created and ready to load";
                callback(sendTxtMessage(phoneParams, callback));
                //process.exit();
            }
        });
        //callback(sendTxtMessage(phoneParams, callback));
        }
    });
    return console.log('Went through function.');
}


function readCSV (i=0,j=0,callback) {
    console.log("in readCSV function");
        const s3Stream = s3.getObject(s3params).createReadStream()
    require('fast-csv').fromStream(s3Stream).on('data', (data) => {
        if (i === 0) {
            tableName = data[0];
            console.log('Table name is: ' + tableName);
        } else if (i === 1) {
            subjectName = data[0];
            console.log('Subject is: ' + subjectName);
        } else if (i === 2) {
            for (var prop in data) {
                columnNames.push(data[prop]);
            }
            var columnOne = columnNames[0];
            var columnTwo = columnNames[1];
            console.log('Column names are: ' + columnNames);
        } else if (i > 2) {
            console.log("Importing question into DynamoDB. Please wait.");
             var params = {
             TableName: tableName,
             Item: {
                Subject:  subjectName + '_' + j,
                columnOne: data[0],
                columnTwo: data[1]
                }
             };
             flashList.push(params);
            console.log(JSON.stringify(params, null, 2));
              
              docClient.put(params, function(err, data) {
               if (err) {
              console.error("Unable to add Subject", subjectName, ". Error JSON:", JSON.stringify(err, null, 2));
               } else {
                  console.log("PutItem succeeded:", subjectName);
               }
                 });

              console.log('Skipping all else for now.');
              j++;
        };
        i++;
    });
    
    phoneParams.Message = "Table loaded";
    callback(sendTxtMessage(phoneParams, callback));
}


exports.handler = (event, context, callback) => {
    //this.tableName = "FlashCards";
    //var a = checkTable.bind(this.tableName);
    checkTable(tableName, callback);
    //setTimeout(function() {console.log(JSON.stringify(flashList, null, 2))},2000);
    //sendTxtMessage(phoneParams, callback);//, myResult=>{
    callback(null, "Read an s3 csv and creating dynamodb table.");
};
