var ws = require('nodejs-websocket');
var Connection = ws.Connection;
var BSONImpl = require('bson');
var BSON = BSONImpl.BSONPure.BSON();

Oneline= {};
Oneline.defaultStateFlags = 
    {
      "CONNECTED": false,
      "STATE": "connecting",
      "LAST_REQUEST": "",
      "LAST_RESPONSE": "",
      "CONNECTION_UUID": "", // this gets forwarded however the broker will also keep its reference
      "UUID": ""  // the last uuid used
    };


Oneline= {};

// Provider, receiver callbacks seperated by their 
// module name if no module name is provided it is executed for any 
// module
//
// Oneline.on('module_name', 'receiver', function(message) {
//  Oneline.log(message);
// });
//
// Oneline.on('receiver', function(message) { 
//   Oneline.log(message);
// });
//
// Oneline.on('open', function(message) {
//   Oneline.log(message); 
// });
//
Oneline.on = function(moduleNameOrType,callback) {
   Oneline.log("Attaching callback for", { "module": moduleNameOrType, "callback": callback});
   var makeSureCallback = arguments[arguments.length-1];
   if(typeof makeSureCallback === 'callback') { // good
      if (moduleNameOrType ===  'receiver' || 
        moduleNameOrType ==='provider' ||
      moduleNameOrType ==='open'  ||
      moduleNameOrType === 'close') {
        Oneline[moduleNameOrType+'Callback']=makeSureCallback;
      } else {
        var messageType = arguments[arguments.length-2];
        if (typeof Oneline[messageType+'Callbacks'][moduleNameOrType] ==='undefined') {
          Oneline[messageType+'Callbacks'][moduleNameOrType]=makeSureCallback;
        } else {
          Oneline.log("You have already registered this callback");
      }
    }
   }
};

    

// add the server ip for the oneline
// server and make sure  this is sent
// with a forwarded request
// @param options a set of broker params
// @successCallback  a callback for any successful connections with the master connection
// @errorCallback  a callback for any errored connections with the master connection

Oneline.listen = function(options,successCallback,errorCallback) {
   //use the module name for what we really use
   Oneline.masterConnections = Oneline.masterConnections || {};
   Oneline.receiverCallbacks = Oneline.receiverCallbacks || {};
   Oneline.providerCallbacks = Oneline.providerCallbacks || {};
   Oneline.brokerUUIDs = Oneline.brokerUUIDs || {};
   Oneline.openCallbacks = Oneline.openCallbacks || {};
   Oneline.closeCallbacks = Oneline.closeCallbacks || {};
   Oneline.receiverCallback = Oneline.receiverCallback || null;
   Oneline.providerCallback =  Oneline.providerCallback || null;
   Oneline.openCallback = Oneline.openCallback || null;
   Oneline.closeCallback = Oneline.closeCallback || null;
   Oneline.connections = Oneline.connections || {}; //seperated by the connection_uuid flag
   Oneline.errorQueue = Oneline.errorQueue || {}; // seperated by moduleNames
   Oneline.successQueue = Oneline.successQueue  || {};  // seperated by moduleNames
   if (typeof options === 'string') {
     var options  = {"moduleName": options };
   } else {
     var options = typeof arguments[0] === 'object' ? arguments[0] : {};
   }
   var errorAndSuccess = typeof arguments[arguments.length-1] ==='function'  && typeof arguments[arguments.length-2] === 'function' ;

   var successCallback = errorAndSuccess ? arguments[arguments.length-2] :arguments[arguments.length-1];
   var errorCallback  = errorAndSuccess ? arguments[arguments.length-1] : function() {}; //noop

   Oneline.successCallbacks = Oneline.successCallbacks || {};
   Oneline.errorCallbacks = Oneline.errorCallbacks || {};
   options.serverHost= options.serverHost || "0.0.0.0";
   options.serverPort = options.serverPort || 9000;
   options.moduleName =  options.moduleName || "Main";
   options.brokerPort = options.brokerPort  || 8999;
   options.brokerHost = options.brokerHost || "0.0.0.0";
   Oneline.connections[options.moduleName]={};   
   Oneline.errorQueue[options.moduleName] = {};
   Oneline.successQueue[options.moduleName]= {};
   Oneline.brokerUUIDs[options.moduleName] =Oneline.uuid();
   Oneline.successCallbacks[options.moduleName] =successCallback;
   Oneline.errorCallbacks[options.moduleName]= errorCallback;

   url = "ws://" +options.serverHost+ ":"+options.serverPort+"/" +options.moduleName;
   Oneline.connectWithMaster(options.moduleName, url, function(error,connection) {
      Oneline.log("Trying to connect with master");
      Oneline.log("Connection status", connection);
     ws.createServer(function(conn) {
       conn.on("listening", function() {
         Oneline.log("Listening to connections on", options);
         Oneline.successCallbacks[moduleName]();
       });
       conn.on("text", function(text) {
          // forward to the master
          // 
          Oneline.log("Receiving message from client");
          var BSONDocument = BSON.deserialize(text, true,false, true); 
          Oneline.registerConnectionIfNeeded(BSONDocument.connection_uuid,conn);
          BSONDocument1 = Oneline.findAndExecuteCallback(BSONDocument.module, Oneline.request(BSONDocument), "provider");
          // process any intermiediate data
          // only if it  is received
          // 
          if (BSONDocument1) {
          var newtext = BSON.serialize(text);
          Oneline.sendToMaster(BSONDocument.module,BSONDocument1,newtext);
          } else {
          Oneline.sendToMaster(BSONDocument.module,BSONDocument,text);
          }
       });
       conn.on("close", function() {
          Oneline.log("Closing connection with client");
       });
       conn.on("error", function() {
          Oneline.log("Connection errored");
        });
      }).listen( options.brokerPort, options.brokerHost );
   }, function() {
      Oneline.log("Closing connection with master");
   }, function() {
      Oneline.log("connection errored");
   });
};

//
Oneline.findAndExecuteErrorQueue = function(moduleName) {
 if (typeof Oneline.errorQueue[moduleName] !== 'undefined') {
    for (var i in Oneline.errorQueue[moduleName]) {
      if (typeof Oneline.errorQueue[moduleName][i] === 'function') {
        Oneline.errorQueue[moduleName][i]();
        delete Oneline.errorQueue[moduleName][i];
      } 
    }
  }
};
//likewise for the successes we have a queue however
//  since the server sends back a message we know of its uuid
// and we only need to execute one time
Oneline.findAndExecuteSuccessQueue = function(moduleName, successUUID, resultMessage){
  if (typeof Oneline.successQueue[moduleName] !== 'undefined' 
      &&  typeof Oneline.successQueue[moduleName][successUUID] !== 'undefined') {
      Oneline.successQueue[moduleName][successUUID](resultMessage);
  }
}

    


// find and execute any of the registered
// callbacks
// @param moduleName: a module name used for the connection
// @param dataResponse: the parsed data response
// @param typeOfCallback:  the type of callback: open|close|receiver|provider
Oneline.findAndExecuteCallback= function(moduleName, dataResponse,typeOfCallback) {
  var primaryResult = false;
  for (var i in  Oneline[typeOfCallback+'Callbacks']) {
        if (i === moduleName) { 
            if (typeof Oneline[typeOfCallback+'Callbacks'][i]  === 'function' ) {
               primaryResult =  Oneline[typeOfCallback+'Callbacks'][i](dataResponse);
            }
        } 
   }
   if (
       Oneline.callbacks[typeofCallback+"Callback"]
    ) { 
      if (!primaryResult) {
       primaryResult = Oneline[typeOfCallback+"Callback"](dataResponse);
       }
    }
    return primaryResult instanceof Oneline.request ? primaryResult.as_obj() :  primaryResult;
};

// find the connection uuid from the  client
// and determine if we need to save its state
// @param moduleName a moduleName to use
// @param connectionMessage the incoming connection message which
// should have our connection_uuid

// register a connection with the broker 
// and do a membership check for any existing
// @param connection_uuid a connection uuid
// @param  the_connection  the internal connection object
Oneline.registerConnectionIfNeeded = function(connection_uuid, the_connection) {
  for (var i in Oneline.connections[moduleName]) {
    if (i === connection_uuid) {
        return Oneline.alreadyConnection();
    }
  }
  Oneline.connections[moduleName][connection_uuid] = { 
        "connection": the_connection,
        "state": {}
  };
};

// exception handling for an already connected 
// connection
//
Oneline.alreadyConnection = function() {
};

// send a raw BSON message to the connection uuid  
// only if it is still around
// @param moduleName the module of the connection
// @param connection_uuid a uuid used in connecting with the broekr
// @param raw_message a  BSON document serialized
Oneline.sendMessageToConnection = function(moduleName, connection_uuid, raw_message) {
  var connection = Oneline.getConnection(moduleName, connection_uuid) ;
  if (connection) {
      connection.sendText(raw_message);
  }  else {
      return Oneline.couldNotFindConnection();
  }
};

// get a connection that has already gone through  
// the initialization process
// @param moduleName the module name associated with the connection
// @param connection_uuid A connection uuid flag passed by the client
//
Oneline.getConnection = function(moduleName,connection_uuid) {
   return typeof Oneline.connections[moduleName][connection_uuid] !== 'undefined' ? Oneline.connections[moduleName][connection_uuid] : false;
};

// update the state for a particular client 
// @param moduleName the module name for the connection
// @param stateIndice  a connection uuid 
// @param stateVariables variables to update with
Oneline.updateStateConnection = function(moduleName,stateIndice, stateVariables) {
  if (typeof Oneline.connections[moduleName][stateIndice]!=='undefined') {
   for (var i in stateVariables) {
    Oneline.connections[moduleName][stateIndice]['state'][i] = stateVariables[i];
   }
  } else {
    Oneline.noStatesFound();
  }
};

// construct a response object from oneline
// this should be similar to the Python ol.response
//
// @return Oneline.response  object
Oneline.response  = function() {
   this.updates = {};
   // set a value for the request
   // @param key anything that is a utf-8 string
   // @param value  anything that is an object string or number
   this.set = function(key,val) {
       this.updates[key]=val;
   };
   //get the value from the set 
   //@param key one of the stored keys
   this.get = function(key) {
      return typeof this.updates[key]!=='undefined'?this.updates[key]:false;
   };
   // serialize the data into a BSON
   // ready document
   this.as_obj = function() {
      return this.updates;
   };

};

// membership check oneOf
// @param member to check for
// @param haystack the array to look in
Oneline.oneOf = function(member, haystack) {
  for(var i in haystack) {
    if(haystack[i]===member) {
     return true;
    }
  }
  return false;
};

// construct a request object
// this should be similar to the Python ol.request
//
// @return Oneline.request object
Oneline.request = function(initialData) {
   this.updates = {};
   // same as Oneline.response.set/2
   //
   this.set  = function(key,val) {
      this.updates[key] = val;
    };
    // get a key from the set
    // treat Oneline.packet differently
    // @param key  anything that is a string
   this.get  = function(key) {
      if (Oneline.oneOf(key, Object.keys(this.updates['packet']))) {
          return typeof this.updates['packet'][key]!=='undefined' ? this.updates['packet'][key] : false;
      } else {
          if(typeof this.updates['packet'][key]!=='undefined') {
          return this.updates[key];
          } else {
          return false;
          }
      }
   };
   // serialize into BSON ready document
   // treat Oneline.request differently
  // 
   this.as_obj  =function() {
     var theData = {}; 
     var theKeys = Object.keys(this.updates);
     for(var i in theKeys) {
      if (typeof this.updates[theKeys[i]] === 'object' && 
          theKeys[i] === "packet") {
        theData[theKeys[i]] =  this.updates[theKeys[i]];
      } else if (typeof this.updates[theKeys[i]]  === "object" &&
          theKeys[i] === "response") {
          theData[theKeys[i]]= this.updates[theKeys[i]].as_obj();
      } else {
         theData[theKeys[i]] =  this.updates[theKeys[i]];
      }
      }
      return theData;
   };
   if(typeof initialData === 'object') {
   var keysOfInitial = Object.keys(initialData);
   for (var i in keysOfInitial) {
     this.set(keysOfInitial[i], initialData[keysOfInitial[i]]);
   }
   }
};

// log a message for the broker and server
//
// @param message a string based message
// @param data any data needing output
Oneline.log = function(message,data) {
   console.log("ONELINE: " + message);
   if(typeof data !== 'undefined' ){
   console.log("ONELINE: data received");
   console.log(data);
   }
};

// get the broker uuid for the moduleName
// we should have a server for each of the modules
// and this should bring a uuid   
// @param moduleName the moduleName of the broker
Oneline.getBrokerUUID=function(moduleName) {
  return typeof Oneline.brokerUUIDs[moduleName] !== 'undefined' ?Oneline.brokerUUIDs[moduleName]: false;
};


// pack a oneline message into its server based readable
// version make it have the connection uuid as well as
// timestamp and the generic object being the one we use
// for the data
// @param moduleName the name of the module for the execution
// @param eventName the eventName for the generic
// @param messageContainer the container of the generic
//
Oneline.packMessage = function(moduleName, eventName,messageContainer) {
   var messageObject = {};
   messageObject.broker_uuid = Oneline.getBrokerUUID(moduleName);
   messageObject.connection_uuid = messageObject.broker_uuid;
   messageObject.uuid = Oneline.uuid();
   messageObject.timestamp = Date.now();
   messageObject.module = moduleName;
   messageObject.packet =  {};
   messageObject.packet.generic = {};
   messageObject.packet.generic = messageContainer;
   return messageObject;
};


// general send. Send a message that will be parsed into a BSON
// document
// @param event the event name associated
// @param object the object to be serialized
// @param successCallback the callback to trigger on an execution
// @param errorCallback the callback to execute on error currently should add
// to the queue as we don't know when we will be hit by a low level error
Oneline.send = function(moduleName,eventName,beforeMessage,successCallback,errorCallback) {
  if(typeof errorCallback !== 'undefined') {
    Oneline.errorQueue[moduleName].push(errorCallback);
   }

  var serializedMessage = BSON.serialize(Oneline.packMessage(moduleName,eventName,beforeMessage));
  Oneline.sendToMaster(moduleName,serializedMessage,beforeMessage);
  // call our call  back considering this has been done
  //
  successCallback();
};

/* generate a uid not more than
 * 1, 000, 000. This code was 'borrowed' from 'KennyTM'
 * @ http://stackoverflow.com/questions/624*666/how-to-generate-short-uid-like-ax4j9z-in-js
 */
Oneline.uuid = function() 
{
      return ("0000" + (Math.random()*Math.pow(36,4) << 0).toString(36)).slice(-4)
};

// called prior to our messaging with the master
// update the last request  and  any state variables
//
// @param moduleName the moduleName used for this request
// @param parsedMessage  parsed BSON message
// @param rawMessage a raw message in BSON
Oneline.sendToMaster = function(moduleName, parsedMessage, rawMessage) {
    //
    Oneline.updateStateConnection(moduleName,parsedMessage.connection_uuid, {
        "STATE": "SENDING",
        "LAST_REQUEST": parsedMessage, 
        "TIMESTAMP": Date.now()
    });
    Oneline.masterConnections[moduleName].sendText(rawMessage);
};

// sent from the MasterConnection to the Broker
// this is called whenever the WebSocket server provides
// us a response. We will forward back to the client
// @param moduleName the module name used for the message
// @param parsedMessage a parsed BSON message
// @param rawMessage a raw message in BSON
Oneline.sendToBroker = function(moduleName, parsedMessage, rawMessage) {
  Oneline.updateStateConnection( moduleName,parsedMessage.connection_uuid, {
      "STATE": "RECEIVING",
      "LAST_RESPONSE": parsedMessage,
      "TIMESTAMP": Date.now()
    });
         
  Oneline.sendMessageToConnection(moduleName, parsedMessage.connection_uuid, message);
};


// add a master connection only when we don't have it already  
// this is if Oneline.listen/3 is called more than once on the same
// module
// @param moduleName the module we're listening on
// @param connection ws.Connection
Oneline.addMasterConnectionIfNeeded = function(moduleName,connectionObj) {
  Oneline.log("Adding new master connection",connectionObj);
  if((typeof Oneline.masterConnections[moduleName] !== 'undefined' 
       && Oneline.masterConnections[moduleName].readyState !== Connection.OPEN)

    ||  (typeof Oneline.masterConnections[moduleName] === 'undefined')) {
      delete Oneline.masterConnections[moduleName];
      Oneline.masterConnections[moduleName] = connectionObj;
   }
};





// port this will receive any data from the
// oneline implentation
//
// @param moduleName used to  identify the master connection
// @param server  full connection url ws://localhost:9000/Main
// @param openCallback executed whenever we open the connection
// @param closeCallback  called  when the connection with the master
// @param  errorCallback called with the error 
//  is closed
Oneline.connectWithMaster = function(moduleName, server,openCallback,closeCallback,errorCallback) {
    Oneline.log("Connecting on: ");
    Oneline.log(server);

    var connection = ws.connect(server,{}, function() {
      Oneline.addMasterConnectionIfNeeded(moduleName,connection); 
      openCallback();
      connection.on("text", function(text) { 
          Oneline.log("Received message from server");
          var BSONDocument  = BSON.deserialize(text, true, false, true);
          Oneline.log(BSONDocument);
          Oneline.findAndExecuteSuccessQueue( moduleName, BSONDocument.uuid, BSONDocument);
          // only when we have the connection uuid
          var BSONDocument1 = Oneline.findAndExecuteCallback(BSONDocument.module, Oneline.message(BSONDocument), "receiver");
          if(BSONDocument1) { 
             var newText = BSON.serialize(BSONDocument1);
             Oneline.sendToBroker(BSONDocument.module, BSONDocument1,  newText);
          } else {
              Oneline.sendToBroker(BSONDocument.module, text);
            }
      });
      connection.on("close", function() {
         Oneline.log("Closing connection with master server");
         closeCallback();
       });
      connection.on("error", function() {
        Oneline.log("Connection errored");
        Oneline.findAndExecuteErrorQueue( moduleName );
         errorCallback();
        });
   });
};


module.exports.Oneline = Oneline;
