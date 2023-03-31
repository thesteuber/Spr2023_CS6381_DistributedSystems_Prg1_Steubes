###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# BrokerMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import datetime
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually
from topic_selector import TopicSelector

# import any other packages you need.
from CS6381_MW import Common
import json
import random

##################################
#       Broker Middleware class
##################################
class BrokerMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.name = None # Name of broker
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.pub = None # will be a ZMQ PUB socket for dissemination
    self.sub = None # will be a ZMQ SUB socket for dissemination
    self.router = None # will be a ZMQ ROUTER socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.upcall_obj = None # handle to appln obj to handle appln-specific data
    self.handle_events = True # in general we keep going thru the event loop
    self.lookup = "" # lookup method for the system
    self.dht_nodes = None # list of all DHT Nodes in system
    self.context = None
    self.discovery = ""

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args, lookup):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("BrokerMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.discovery = args.discovery
      self.lookup = lookup
      self.name = args.name

      # Next get the ZMQ context
      self.logger.debug ("BrokerMW::configure - obtain ZMQ context")
      self.context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("BrokerMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REQ and PUB sockets
      # REQ is needed because we are the client of the Discovery service
      # PUB is needed because we publish topic data
      self.logger.debug ("BrokerMW::configure - obtain REQ and PUB sockets")
      self.pub = self.context.socket (zmq.PUB)
      self.sub = self.context.socket (zmq.SUB)

      if (self.lookup == "Chord"):
        self.router = self.context.socket(zmq.ROUTER)
        self.router.bind("tcp://*:{}".format(self.port))

        # load in DHT Nodes from json file
        with open(args.dht_json) as json_file:
            self.dht_nodes = json.load(json_file).get('dht')
      else:
        self.req = self.context.socket (zmq.REQ)
        # Since are using the event loop approach, register the REQ socket for incoming events
        # Note that nothing ever will be received on the PUB socket and so it does not make
        # any sense to register it with the poller for an incoming message.
        self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
        self.poller.register (self.req, zmq.POLLIN)
      
        # Now connect ourselves to the discovery service. Recall that the IP/port were
        # supplied in our argument parsing. Best practices of ZQM suggest that the
        # one who maintains the REQ socket should do the "connect"
        self.logger.debug ("BrokerMW::configure - connect to Discovery service")
        # For our assignments we will use TCP. The connect string is made up of
        # tcp:// followed by IP addr:port number.
        connect_str = "tcp://" + args.discovery
        self.req.connect (connect_str)
      
      # Since we are the broker and must act as publisher as well as subscriber, the best practice as suggested in ZMQ is for us to
      # "bind" the PUB socket
      self.logger.debug ("BrokerMW::configure - bind to the pub socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      # Since port is an integer, we convert it to string to make it part of the URL
      bind_string = "tcp://*:" + str(self.port)
      self.pub.bind (bind_string)
      
      self.logger.info ("BrokerMW::configure completed")

    except Exception as e:
      raise e

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self, timeout=None):

    try:
      self.logger.info ("BrokerMW::event_loop - run the event loop")

      # we are using a class variable called "handle_events" which is set to
      # True but can be set out of band to False in order to exit this forever
      # loop
      while self.handle_events:  # it starts with a True value
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict (self.poller.poll (timeout=timeout))

        # Unlike the previous starter code, here we are never returning from
        # the event loop but handle everything in the same locus of control
        # Notice, also that after handling the event, we retrieve a new value
        # for timeout which is used in the next iteration of the poll
        
        # check if a timeout has occurred. We know this is the case when
        # the event mask is empty
        if not events:
          # timeout has occurred so it is time for us to make appln-level
          # method invocation. Make an upcall to the generic "invoke_operation"
          # which takes action depending on what state the application
          # object is in.
          timeout = self.upcall_obj.invoke_operation ()
          
        elif self.req in events:  # this is the only socket on which we should be receiving replies

          # handle the incoming reply from remote entity and return the result
          timeout = self.handle_reply ()
          
        else:
          raise Exception ("Unknown event after poll")

      self.logger.info ("BrokerMW::event_loop - out of the event loop")
    except Exception as e:
      raise e
            
  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply (self):

    try:
      self.logger.info ("BrokerMW::handle_reply")

      # let us first receive all the bytes
      bytesRcvd = None
      if (self.lookup == "Chord"):
        identity, bytesRcvd = self.router.recv_multipart()
      else:
        bytesRcvd = self.req.recv ()

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here DiscoveryResp and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.ParseFromString (bytesRcvd)

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so. See how we make
      # the upcall on the application object by using the saved handle to the appln object.
      #
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.
      if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
        # let the appln level object decide what to do
        timeout = self.upcall_obj.register_response (disc_resp.register_resp)
      elif (disc_resp.msg_type == discovery_pb2.TYPE_UNREGISTER):
        # let the appln level object decide what to do
        timeout = self.upcall_obj.unregister_response (disc_resp.register_resp)
      elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
        self.logger.error ("BrokerMW::handle_reply TYPE_ISREADY is deprecated!")
        # this is a response to is ready request
        # timeout = self.upcall_obj.isready_response (disc_resp.isready_resp)
      elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
        # this is a response to is ready request
        timeout = self.upcall_obj.lookup_all_pubs_response (disc_resp.allpubs_resp)

      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError ("Unrecognized response message")

      return timeout
    
    except Exception as e:
      raise e
            
  def send_to_discovery_services(self, disc_req):
    # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
    # a real string
    buf2send = disc_req.SerializeToString ()
    self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

    if (self.lookup == "Chord"):
      self.logger.debug ("SubscriberMW::send_to_discovery_services - CHORD send to random Discovery service")
      #create ZMQ req socket for successor
      req_context = zmq.Context ()  # returns a singleton object
      tmp_req = req_context.socket (zmq.REQ)

      random_discovery_node = random.choice(self.dht_nodes)

      connect_str = "tcp://" + random_discovery_node['IP'] + ":" + random_discovery_node['port']
      tmp_req.connect (connect_str)
      self.logger.info ("DiscoveryMW::send_to_ip_port successor connected to {}".format(connect_str))

      # now send this to our discovery service
      identity = f"{self.addr}:{self.port}".encode()
      tmp_req.send_multipart ([identity, buf2send])  # we use the "send" method of ZMQ that sends the bytes
      tmp_req.close()
    else:
      # now send this to our discovery service
      self.logger.debug ("BrokerMW::send_to_discovery_services - send stringified buffer to Discovery service")
      
      while True:
        try:
            # Initialize REQ socket if not already done
            if self.req is None:
                context = zmq.Context()
                self.req = context.socket(zmq.REQ)
                self.req.connect("tcp://" + self.discovery)
                self.poller.register(self.req, zmq.POLLIN)
            
            # Send registration request
            self.req.send(buf2send)

            # Wait for response
            socks = dict(self.poller.poll(4000))
            if self.req in socks and socks[self.req] == zmq.POLLIN:
                self.logger.debug ("BrokerMW::send_to_discovery_services - response received from Discovery service")
                return True
            
            # Timeout waiting for response, close and re-open REQ socket
            self.logger.debug ("BrokerMW::send_to_discovery_services - timed out waiting for response from Discovery service, re-sending registration request")
            self.req.close()
            self.req = None
            self.poller.unregister(self.req)
            
        except Exception as e:
            self.logger.error("Exception caught in BrokerMW::send_to_discovery_services: {}".format(e))
            return False

  ########################################
  # register with the discovery service
  #
  # this method is invoked by application object passing the necessary
  # details but then as a middleware object it is our job to do the
  # serialization using the protobuf generated code
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################
  def register (self, name, topiclist):
    ''' register the appln with the discovery service '''

    try:
      self.logger.info ("BrokerMW::register")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are publishing,
      # and our whereabouts, e.g., name, IP and port

      # The following code shows serialization using the protobuf generated code.

      # Build the Registrant Info message first.
      self.logger.debug ("BrokerMW::register - populate the Registrant Info")
      reg_info = discovery_pb2.RegistrantInfo () # allocate
      reg_info.id = name  # our id
      reg_info.addr = self.addr  # our advertised IP addr where we are publishing
      reg_info.port = self.port # port on which we are publishing
      self.logger.debug ("BrokerMW::register - done populating the Registrant Info")
      
      # Next build a RegisterReq message
      self.logger.debug ("BrokerMW::register - populate the nested register req")
      register_req = discovery_pb2.RegisterReq ()  # allocate 
      register_req.role = discovery_pb2.ROLE_BOTH  # we are a publisher/subscriber as the broker
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
      register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
      self.logger.debug ("BrokerMW::register - done populating nested RegisterReq")

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("BrokerMW::register - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (register_req)
      self.logger.debug ("BrokerMW::register - done building the outer message")
      
      self.send_to_discovery_services(disc_req)

      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::register - sent register message and now now wait for reply")
    
    except Exception as e:
      raise e
    
  def unregister (self, name, topiclist):
    ''' register the appln with the discovery service '''

    try:
      self.logger.info ("BrokerMW::unregister")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are publishing,
      # and our whereabouts, e.g., name, IP and port

      # The following code shows serialization using the protobuf generated code.

      # Build the Registrant Info message first.
      self.logger.debug ("BrokerMW::unregister - populate the Registrant Info")
      reg_info = discovery_pb2.RegistrantInfo () # allocate
      reg_info.id = name  # our id
      reg_info.addr = self.addr  # our advertised IP addr where we are publishing
      reg_info.port = self.port # port on which we are publishing
      self.logger.debug ("BrokerMW::unregister - done populating the Registrant Info")
      
      # Next build a RegisterReq message
      self.logger.debug ("BrokerMW::unregister - populate the nested register req")
      register_req = discovery_pb2.UnregisterReq ()  # allocate 
      register_req.role = discovery_pb2.ROLE_BOTH  # we are a publisher/subscriber as the broker
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
      register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
      self.logger.debug ("BrokerMW::unregister - done populating nested RegisterReq")

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("BrokerMW::unregister - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.TYPE_UNREGISTER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.unregister_req.CopyFrom (register_req)
      self.logger.debug ("BrokerMW::unregister - done building the outer message")
      
      self.send_to_discovery_services(disc_req)

      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::unregister - sent register message and now now wait for reply")
    
      self.handle_reply ()

    except Exception as e:
      raise e

  ########################################
  # check if the discovery service gives us a green signal to proceed
  #
  # Here we send the isready message and do the serialization
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################
  def is_ready (self):
    ''' register the appln with the discovery service '''

    try:
      self.logger.info ("BrokerMW::is_ready")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("BrokerMW::is_ready - populate the nested IsReady msg")
      isready_req = discovery_pb2.IsReadyReq ()  # allocate 
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("BrokerMW::is_ready - done populating nested IsReady msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("BrokerMW::is_ready - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.isready_req.CopyFrom (isready_req)
      self.logger.debug ("BrokerMW::is_ready - done building the outer message")
      
      self.send_to_discovery_services(disc_req)
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::is_ready - request sent and now wait for reply")
      
    except Exception as e:
      raise e
 
  ########################################
  # get all pubs from discovery service
  ########################################
  def lookup_all_pubs (self):
    ''' register the appln with the discovery service '''

    try:
      self.logger.info ("BrokerMW::lookup_all_pubs")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a LookupAllPubsReq message
      self.logger.debug ("BrokerMW::lookup_all_pubs - populate the nested LookupAllPubsReq msg")
      allpubs_req = discovery_pb2.LookupAllPubsReq ()  # allocate 
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("BrokerMW::lookup_all_pubs - done populating nested LookupAllPubsReq msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("BrokerMW::lookup_all_pubs - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.allpubs_req.CopyFrom (allpubs_req)
      self.logger.debug ("BrokerMW::lookup_all_pubs - done building the outer message")
      
      self.send_to_discovery_services(disc_req)
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::lookup_all_pubs - request sent and now wait for reply")
      
    except Exception as e:
      raise e

  #################################################################
  # connect a given publisher to the ZMQ SUB socket associated with 
  # this broker.
  #################################################################
  def connect_to_publisher(self, publisher):
    try:
      self.logger.debug ("BrokerMW::connect_to_publisher " + str(publisher.id))
      self.logger.debug ("tcp://{}:{}".format(publisher.addr, publisher.port))

      # connect the publisher to the SUB ZMQ Socket
      self.sub.connect("tcp://{}:{}".format(publisher.addr, publisher.port))
      ts = TopicSelector ()
      allTopics = ts.interest (9)
      for t in allTopics:
        self.sub.subscribe(t)
      
      self.logger.debug ("BrokerMW::connect_to_publisher complete")
    except Exception as e:
      raise e

  #################################################################
  # collect the data on our pub socket
  #
  # do the actual dissemination of info using the ZMQ pub socket
  #
  # Note, here I am sending three diff params. I am eventually going to replace this
  # sending of a simple string with protobuf serialization. Recall that we need to be
  # sending publisher id, topic, data, timestamp at a minimum for our experimental
  # data collection. So anyway we will need to do the necessary serialization.
  #
  # This part is left as an exercise.
  #################################################################
  def collect (self):
    try:
      self.logger.debug ("BrokerMW::collect")

      # receive the info as bytes. See how we are providing an encoding of utf-8
      message = self.sub.recv_string()

      self.logger.debug ("BrokerMW::collect complete")

      return message
      
    except Exception as e:
      raise e

  #################################################################
  # disseminate the data on our pub socket
  #
  # do the actual dissemination of info using the ZMQ pub socket
  #
  # Note, here I am sending three diff params. I am eventually going to replace this
  # sending of a simple string with protobuf serialization. Recall that we need to be
  # sending publisher id, topic, data, timestamp at a minimum for our experimental
  # data collection. So anyway we will need to do the necessary serialization.
  #
  # This part is left as an exercise.
  #################################################################
  def disseminate (self, message):
    try:
      self.logger.debug ("BrokerMW::disseminate")
      
      self.logger.debug ("BrokerMW::disseminate - {}".format (message))
      self.logger.debug ("BrokerMW::disseminate - " + str(self.addr) + ":" + str(self.port))

      # send the info as bytes. See how we are providing an encoding of utf-8
      self.pub.send (bytes(message, "utf-8"))

      self.logger.debug ("BrokerMW::disseminate complete")
    except Exception as e:
      raise e
            
  ########################################
  # set upcall handle
  #
  # here we save a pointer (handle) to the application object
  ########################################
  def set_upcall_handle (self, upcall_obj):
    ''' set upcall handle '''
    self.upcall_obj = upcall_obj

  ########################################
  # disable event loop
  #
  # here we just make the variable go false so that when the event loop
  # is running, the while condition will fail and the event loop will terminate.
  ########################################
  def disable_event_loop (self):
    ''' disable event loop '''
    self.handle_events = False