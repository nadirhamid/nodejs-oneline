var ws = require('nodejs-websocket');
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
//  console.log(message);
// });
//
// Oneline.on('receiver', function(message) { 
//   console.log(message);
// });
//
// Oneline.on('open', function(message) {
//   console.log(message); 
// });
//
Oneline.on = function(moduleNameOrType,callback) {
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
          console.log("You have already registered this callback");
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
   Oneline.server = Oneline.server || ws;
   Oneline.masterConnections = Oneline.masterConnections || {};
   Oneline.receiverCallbacks = Oneline.receiverCallbacks || {};
   Oneline.providerCallbacks = Oneline.providerCallbacks || {};
   Oneline.openCallbacks = Oneline.openCallbacks || {};
   Oneline.closeCallbacks = Oneline.closeCallbacks || {};
   Oneline.receiverCallback = Oneline.receiverCallback || null;
   Oneline.providerCallback =  Oneline.providerCallback || null;
   Oneline.openCallback = Oneline.openCallback || null;
   Oneline.closeCallback = Oneline.closeCallback || null;
   Oneline.connections = Oneline.connections || {}; //seperated by the connection_uuid flag
   Oneline.states = Oneline.states || {};
   if (typeof options === 'string') {
     var options  = {"moduleName": options };
   } else {
     var options = typeof arguments[0] === 'object' ? arguments[0] : {};
   }
   options.successCallback = arguments[arguments.length-2];
   options.errorCallback  = arguments[arguments.length-1];
   options.serverHost= options.serverHost || "0.0.0.0";
   options.serverPort = options.serverPort || 9000;
   options.moduleName =  options.moduleName || "Main";
   options.brokerPort = options.brokerPort  || 8999;
   

   url = "ws://" +options.serverHost+ ":"+options.serverPort+"/" +options.moduleName;
   Oneline.connectWithMaster(options.moduleName, url, function(error,connection) {
      console.log("Trying to connect with master");
     Oneline.server.createServer(function(conn) {
       conn.on("open", function(connection) {
          console.log("Opening connection with client");
          options.successCallback();
       });
       conn.on("text", function(text) {
          // forward to the master
          // 
          console.log("Receiving message from client");
          var BSONDocument = BSON.deserialize(text, true,false, true); 
          Oneline.registerConnectionIfNeeded(BSONDocument.connection_uuid,conn);
          Oneline.findAndExecuteCallback(BSONDocument.module, BSONDocument, "provider");
          Oneline.sendToMaster(BSONDocument.module,BSONDocument,text);
       });
       conn.on("close", function() {
          console.log("Closing connection with client");
       });
       conn.on("error", function() {
          console.log("Connection errored");
        });
      }).listen( options.brokerPort );
   }, function() {
      console.log("Closing connection with master");
   }, function() {
      console.log("connection errored");
   });
};

// find and execute any of the registered
// callbacks
// @param moduleName: a module name used for the connection
// @param dataResponse: the parsed data response
// @param typeOfCallback:  the type of callback: open|close|receiver|provider
Oneline.findAndExecuteCallback= function(moduleName, dataResponse,typeOfCallback) {
  for (var i in  Oneline[typeOfCallback+'Callbacks']) {
        if (i === moduleName) { 
            if (typeof i  === 'function' ) {
               Oneline[typeOfCallback+'Callbacks'](dataResponse);
            }
        } 
   }
   if (
       Oneline.callbacks[typeofCallback+"Callback"]
    ) { 
     Oneline[typeOfCallback+"Callback"](dataResponse);
    }
};

// register a connection with the broker 
// and do a membership check for any existing
// @param connection_uuid a connection uuid
// @param  the_connection  the internal connection object
Oneline.registerConnectionIfNeeded = function(connection_uuid, the_connection) {
  for (var i in Oneline.connections) {
    if (i === connection_uuid) {
        return Oneline.alreadyConnected();
    }
  }
  Oneline.connections[connection_uuid] =the_connection;
  Oneline.states[connection_uuid] = Oneline.defaultStateFalgs;
};

// exception handling for an already connected 
// connection
//
Oneline.alreadyConnection = function() {
};

// send a raw BSON message to the connection uuid  
// only if it is still around
// @param connection_uuid a uuid used in connecting with the broekr
// @param raw_message a  BSON document serialized
Oneline.sendMessageToConnection = function(connection_uuid, raw_message) {
  var connection = Oneline.getConnection(connection_uuid);
  if (connection) {
      connection.sendText(raw_message);
  }  else {
      return Oneline.couldNotFindConnection();
  }
};

// get a connection that has already gone through  
// the initialization process
// @param connection_uuid A connection uuid flag passed by the client
//
Oneline.getConnection = function(connection_uuid) {
   return typeof Oneline.connections[connection_uuid] !== 'undefined' ? Oneline.connections[connection_uuid] : false;
};

// update the state for a particular client 
// @param stateIndice  a connection uuid 
// @param stateVariables variables to update with
Oneline.updateStateConnection = function(stateIndice, stateVariables) {
  if (typeof Oneline.states[stateIndice]!=='undefined') {
   for (var i in stateVariables) {
    Oneline.states[stateIndice][i] = stateVariables[i];
   }
  } else {
    Oneline.noStatesFound();
  }
};

// called prior to our messaging with the master
// update the last request  and  any state variables
//
// @param moduleName the moduleName used for this request
// @param parsedMessage  parsed BSON message
// @param rawMessage a raw message in BSON
Oneline.sendToMaster = function(moduleName, parsedMessage, rawMessage) {
    //
    Oneline.updateStateConnection(parsedMessage.connection_uuid, {
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
  Oneline.updateStateConnection( parsedMessage.connection_uuid, {
      "STATE": "RECEIVING",
      "LAST_RESPONSE": parsedMessage,
      "TIMESTAMP": Date.now()
    });
         
  Oneline.sendMessageToConnection(parsedMessage.connection_uuid, message);
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
    console.log("Connecting on: ");
    console.log(server);
      Oneline.masterConnections[moduleName] = conn = ws.connect(server,{});
    conn.on("open", function(connection) {
       console.log("Opening connection with master server");
       conn.sendText(BSON.serialize(openingMessage));
       openCallback();
    });
    conn.on("text", function(text) { 
        console.log("Received message from server");
        var BSONDocument  = BSON.deserialize(text, true, false, true);
        console.log(BSONDocument);

        // only when we have the connection uuid
        Oneline.findAndExecuteCallback(BSONDocument.module, BSONDocument, "receiver");
        
        Oneline.sendToBroker(BSONDocument.module, text);
    });
    conn.on("close", function() {
       console.log("Closing connection with master server");
       closeCallback();
     });
    conn.on("error", function() {
      console.log("Connection errored");
       errorCallback();
      });
      

};


module.exports.Oneline = Oneline;
