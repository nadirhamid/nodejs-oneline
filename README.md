NodeJS Oneline Broker
=============================================================

This provides a Broker implementation for (http://getoneline.com/).
It is used directly in between the client and server and can fully act as either.

Info
-------------------------------------------------------------

The NodeJS broker can interchange, act or even become the server and
client it does so by providing a proxy layer to the client and server
connection. Similar to most broker servers it also communicates directly
in the interchange language used by the Server and Client (BSON).




Example Usage
-------------------------------------------------------------
   
  Oneline.listen("MyModule", function() {
      Oneline.on("receiver", function(message) {
        // this is a message being received from the server
        console.log(message.connection_uuid); // the sender
        console.log(message.module); // this should give us MyModule
        console.log(message.packet); // this should give us the packet object
          return message;
      });
      Oneline.on("provider", function(message) {
        // this is a message being sent to the server
       return message;
      });
      Oneline.on("open", function() {
        // on an opened connection
      });
      Oneline.on("end", function() {
       // on an ended connection
      });
    });

You can subscribe to multiple modules

  Oneline.listen("Module1", function() {
      // baseline setup for Module1
  });
  Oneline.listen("Module2", function() {
      // baseline setup for Module2
  });






More
----------------------------------------------------------

http://getoneline.com/libraries/nodejs_broker

More on Oneline Brokers:
http://getoneline.com/libraries/

More on Oneline:
https://pypi.python.org/pypi/Oneline/1.0.0RC1
