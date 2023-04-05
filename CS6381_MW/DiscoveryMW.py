###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
from DiscoveryLedger import DiscoveryLedger, Registrant

#from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Discovery Middleware class
##################################
class DiscoveryMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.name = None # Name of discoverer
    self.rep = None # will be a ZMQ REP socket to talk to Discovery service
    self.broker_req_socket = None # will be a ZMQ REP socket to talk to Broker service
    self.sub_req_sockets = {} # will be a ZMQ REP socket to talk to each subscriber
    self.router = None # will be a ZMQ ROUTER socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.upcall_obj = None # handle to appln obj to handle appln-specific data
    self.handle_events = True # in general we keep going thru the event loop
    self.lookup = "" # lookup method for the system
    self.context = None

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args, lookup):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("DiscoveryMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      

      self.logger.debug ("DiscoveryMW::configure - Location: {}:{}".format(self.addr, self.port))

      self.lookup = lookup
      self.name = args.name
      
      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      self.context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("DiscoveryMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REP socket
      # REP is needed because we are the responder of the Discovery service
      self.logger.debug ("DiscoveryMW::configure - obtain REP sockets")

      if (self.lookup == "Chord"):
        self.router = self.context.socket(zmq.ROUTER)
        self.router.bind("tcp://*:{}".format(self.port))
        self.poller.register(self.router, zmq.POLLIN)

      else:
        self.rep = self.context.socket (zmq.REP)
        # Since we are using the event loop approach, register the REP socket for incoming events
        self.logger.debug ("DiscoveryMW::configure - register the REP socket for incoming replies")
        self.poller.register (self.rep, zmq.POLLIN)

        self.rep.bind("tcp://*:{}".format(self.port))
      
      self.logger.info ("DiscoveryMW::configure completed")

    except Exception as e:
      raise e

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self, timeout=None):

    try:
      self.logger.info ("DiscoveryMW::event_loop - run the event loop")

      # we are using a class variable called "handle_events" which is set to
      # True but can be set out of band to False in order to exit this forever
      # loop
      while self.handle_events:  # it starts with a True value
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        if (self.lookup == 'Chord'):
          timeout = self.handle_reply()
        else:
          events = dict (self.poller.poll (timeout=timeout))
          
          if self.rep in events:  
            # handle the incoming reply from remote entity and return the result
            timeout = self.handle_reply()

      self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
    except Exception as e:
      raise e
            
  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply(self):

    try:
      self.logger.info ("DiscoveryMW::handle_reply")

      # let us first receive all the bytes
      disc_req = discovery_pb2.DiscoveryReq()
      
      bytesRcvd = None
      ip = ""
      port = ""
      if (self.lookup == "Chord"):
        self.logger.info ("DiscoveryMW::handle_reply CHORD receive multipart")
        identiy, _, _, bytesRcvd = self.router.recv_multipart()
        identiy = identiy.decode()
        self.logger.info ("DiscoveryMW::handle_reply CHORD identity {}".format(identiy))
        ip, port = identiy.split(":")
      else:
        bytesRcvd = self.rep.recv()
        last_endpoint = self.rep.getsockopt(zmq.LAST_ENDPOINT).decode('utf-8')
        ip, port = last_endpoint.strip("tcp://").split(':')

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here DiscoveryResp and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_req.ParseFromString(bytesRcvd)

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so. See how we make
      # the upcall on the application object by using the saved handle to the appln object.
      #
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.
      if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
        # let the appln level object decide what to do
        timeout = self.upcall_obj.register_request (disc_req.register_req, ip, port)
      elif (disc_req.msg_type == discovery_pb2.TYPE_UNREGISTER):
        # let the appln level object decide what to do
        timeout = self.upcall_obj.unregister_request (disc_req.unregister_req, ip, port)
      elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is the is ready request
        self.logger.error("DiscoveryMW::handle_reply received ISREADY request, but that is deprecated...")
        # timeout = self.upcall_obj.isready_request(ip, port)
      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a lookup publishers by topic/s request
        timeout = self.upcall_obj.lookup_by_topic_request(disc_req.lookup_req, ip, port)
      elif (disc_req.msg_type == discovery_pb2.CHORD_TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a lookup publishers by topic/s request
        timeout = self.upcall_obj.chord_lookup_by_topic_request(disc_req.chord_lookup_req)
      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
        # this is a lookup all publishers request
        timeout = self.upcall_obj.lookup_all_pubs(ip, port)
      elif (disc_req.msg_type == discovery_pb2.CHORD_TYPE_LOOKUP_ALL_PUBS):
        # this is a lookup all publishers request
        timeout = self.upcall_obj.chord_lookup_all_pubs(disc_req.chord_allpubs_req)
      elif (disc_req.msg_type == discovery_pb2.INC_REG_PUBS):
        # increment the registered pubs count and send the message on
        timeout = self.upcall_obj.increment_registered_pubs(disc_req)
      elif (disc_req.msg_type == discovery_pb2.INC_REG_SUBS):
        # increment the registered subs count and send the message on
        timeout = self.upcall_obj.increment_registered_subs(disc_req)
      elif (disc_req.msg_type == discovery_pb2.CHORD_SET_BROKER):
        # Set Broker Across the Ring
        timeout = self.upcall_obj.chord_set_broker(disc_req)

      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError ("Unrecognized response message")

      return timeout
    
    except Exception as e:
      raise e

  def send_message(self, socket, msg, ip, port):
    # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
    # a real string
    buf2send = msg.SerializeToString ()
    self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

    if (self.lookup == "Chord"):
      self.logger.debug ("SubscriberMW::send_message")
      #create ZMQ req socket for successor
      req_context = zmq.Context ()  # returns a singleton object
      tmp_req = req_context.socket (zmq.REQ)

      connect_str = "tcp://" + ip + ":" + port
      tmp_req.connect (connect_str)
      self.logger.info ("DiscoveryMW::send_to_ip_port successor connected to {}".format(connect_str))

      # now send this to our discovery service
      identity = f"{self.addr}:{self.port}".encode()
      tmp_req.send_multipart ([identity, buf2send])  # we use the "send" method of ZMQ that sends the bytes
      tmp_req.close()
    else:
      # now send this to our discovery service
      socket.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

  ########################################
  # Send is register status back to requester
  ########################################
  def send_register_status (self, success, reason, ip, port):
    ''' send the register status '''
    try:
      self.logger.info ("DiscoveryMW::send_register_status")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_register_status - populate the nested Register Response msg")
      reg_resp = discovery_pb2.RegisterResp ()  # allocate 
      reg_resp.status = discovery_pb2.STATUS_SUCCESS if success else discovery_pb2.STATUS_FAILURE
      reg_resp.reason = reason
      
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::send_register_status - done populating nested Register Response msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::send_register_status - build the outer Register Response message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_REGISTER
      
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.register_resp.CopyFrom (reg_resp)
      self.logger.debug ("DiscoveryMW::send_register_status - done building the outer message")
      
      self.send_message(self.rep, disc_resp, ip, port)
      
    except Exception as e:
      raise e
    
  def send_unregister_status (self, success, reason, ip, port):
    ''' send the register status '''
    try:
      self.logger.info ("DiscoveryMW::send_unregister_status")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_unregister_status - populate the nested Register Response msg")
      unreg_resp = discovery_pb2.UnregisterResp ()  # allocate 
      unreg_resp.status = discovery_pb2.STATUS_SUCCESS if success else discovery_pb2.STATUS_FAILURE
      unreg_resp.reason = reason
      
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::send_unregister_status - done populating nested Register Response msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::send_unregister_status - build the outer Register Response message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_UNREGISTER
      
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.unregister_resp.CopyFrom (unreg_resp)
      self.logger.debug ("DiscoveryMW::send_unregister_status - done building the outer message")
      
      self.send_message(self.rep, disc_resp, ip, port)
      
    except Exception as e:
      raise e
    
  def send_register_status_to (self, success, reason, ip, port):
    ''' send the register status '''
    try:
      self.logger.info ("DiscoveryMW::send_register_status")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_register_status - populate the nested Register Response msg")
      reg_resp = discovery_pb2.RegisterResp ()  # allocate 
      reg_resp.status = discovery_pb2.STATUS_SUCCESS if success else discovery_pb2.STATUS_FAILURE
      reg_resp.reason = reason
      
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::send_register_status - done populating nested Register Response msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::send_register_status - build the outer Register Response message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_REGISTER
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.register_resp.CopyFrom (reg_resp)
      self.logger.debug ("DiscoveryMW::send_register_status - done building the outer message")
      
      self.send_message(self.rep, disc_resp, ip, port)
      
    except Exception as e:
      raise e


  def forward_register_req_to_node(self, reg_req, node):
    try:
      self.logger.info ("DiscoverlyMw::forward_register_req_to_node")
      
      self.logger.info ("DiscoverlyMw::forward_register_req_to_node get wrapped in discovery request")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.RegisterReq  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (reg_req)
      self.logger.debug ("DiscoverlyMw::forward_register_req_to_node - done building the disc_req")

      self.pass_to_successor(disc_req, node)

    except Exception as e:
      raise e

  def get_set_broker_req (self, sender_hash, info):
    try:
      self.logger.info ("DiscoverlyMw::get_set_broker_req")

      # Next build a IncrementRegisteredPubsReq message
      self.logger.debug ("DiscoverlyMw::get_set_broker_req - Create the request")
      chord_setbroker_req = discovery_pb2.ChordSetBrokerReq ()  # allocate 
      chord_setbroker_req.sender_hash = sender_hash
      chord_setbroker_req.info = info

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("DiscoverlyMw::get_set_broker_req - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.CHORD_SET_BROKER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.chord_setbroker_req.CopyFrom (chord_setbroker_req)
      self.logger.debug ("DiscoverlyMw::get_set_broker_req - done building the outer message")
      
      return disc_req
    
    except Exception as e:
      raise e

  def broker_leader_handle(self, ip, port):
    broker = Registrant("new broker", ip, port, None)
    self.set_broker_leader(broker)

  def set_broker_leader (self, broker):
      self.logger.info ("DiscoverlyMw::set_broker_leader")
      if self.broker_req_socket != None:
        self.broker_req_socket.close()
      self.broker_req_socket = self.context.socket(zmq.REQ)
      self.broker_req_socket.connect(f"tcp://{broker.address}:{broker.port + 1}")
      self.upcall_obj.refresh_leading_broker_publishers()
      mylist = self.get_disc_resp_send_topic_publishers([broker])
      self.refresh_subscribers_publishers(mylist)
      self.logger.info ("DiscoverlyMw::set_broker_leader - completed")

  def add_sub_req_socket(self, sub):
    self.logger.info ("DiscoverlyMw::add_sub_req_socket")
    tcp_address = f"tcp://{sub.address}:{sub.port + 1}"

    if tcp_address not in self.sub_req_sockets.keys:
      sub_socket = self.context.socket(zmq.REQ)
      sub_socket.connect(tcp_address)
      self.sub_req_sockets[tcp_address] = sub_socket
      self.logger.info ("DiscoverlyMw::add_sub_req_socket - completed, added {}".format(tcp_address))
    else:
      self.logger.info ("DiscoverlyMw::add_sub_req_socket - completed, {} already in dictionary".format(tcp_address))

  def remove_sub_req_socket(self, sub):
    self.logger.info ("DiscoverlyMw::remove_sub_req_socket")
    tcp_address = f"tcp://{sub.address}:{sub.port + 1}"
    if tcp_address in self.sub_req_sockets.keys:
        sub_socket = self.sub_req_sockets[tcp_address]
        sub_socket.close()
        del self.sub_req_sockets[tcp_address]
        self.logger.info ("DiscoverlyMw::remove_sub_req_socket - completed, removed {}".format(tcp_address))
    else:
      self.logger.info ("DiscoverlyMw::remove_sub_req_socket - completed, {} already not in dictionary".format(tcp_address))

  def refresh_subscribers_publishers(self, msg):
    self.logger.info ("DiscoverlyMw::refresh_subscribers_publishers")

    for sub_socket in self.sub_req_sockets:
      self.logger.info ("DiscoverlyMw::refresh_subscribers_publishers - sending msg")
      self.send_message(sub_socket, msg, None, None)

    self.logger.info ("DiscoverlyMw::refresh_subscribers_publishers - completed")

  def get_increment_pub_req (self, sender_hash):
    try:
      self.logger.info ("DiscoverlyMw::get_increment_pub_req")

      # Next build a IncrementRegisteredPubsReq message
      self.logger.debug ("DiscoverlyMw::get_increment_pub_req - Create the request")
      incregpubs_req = discovery_pb2.IncrementRegisteredPubsReq ()  # allocate 
      incregpubs_req.sender_hash = sender_hash

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("DiscoverlyMw::get_increment_pub_req - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.INC_REG_PUBS  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.incregpubs_req.CopyFrom (incregpubs_req)
      self.logger.debug ("DiscoverlyMw::get_increment_pub_req - done building the outer message")
      
      return disc_req
    
    except Exception as e:
      raise e
    
  def get_increment_sub_req (self, sender_hash):
    try:
      self.logger.info ("DiscoverlyMw::get_increment_sub_req")

      # Next build a IncrementRegisteredPubsReq message
      self.logger.debug ("DiscoverlyMw::get_increment_sub_req - Create the request")
      incregsubs_req = discovery_pb2.IncrementRegisteredSubsReq ()  # allocate 
      incregsubs_req.sender_hash = sender_hash

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("DiscoverlyMw::get_increment_sub_req - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.INC_REG_SUBS  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.incregsubs_req.CopyFrom (incregsubs_req)
      self.logger.debug ("DiscoverlyMw::get_increment_sub_req - done building the outer message")
      
      return disc_req
    
    except Exception as e:
      raise e

  def send_to_ip_port(self, msg, ip, port):
    try: 
      self.logger.info ("DiscoveryMW::send_to_ip_port")
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = msg.SerializeToString ()
      self.logger.debug ("DiscoveryMW::send_to_ip_port Stringified serialized buf = {}".format (buf2send))

      #create ZMQ req socket for successor
      req_context = zmq.Context ()  # returns a singleton object
      tmp_req = req_context.socket (zmq.REQ)

      connect_str = "tcp://" + ip + ":" + port
      tmp_req.connect (connect_str)
      self.logger.info ("DiscoveryMW::send_to_ip_port successor connected to {}".format(connect_str))

      # now send this to our discovery service
      identity = f"{self.addr}:{self.port}".encode()
      tmp_req.send_multipart ([identity, buf2send])   # we use the "send" method of ZMQ that sends the bytes
      tmp_req.close()
    except Exception as e:
      raise e

  def pass_to_successor(self, disc_req, successor):
    try: 
      self.logger.info ("DiscoveryMW::pass_to_successor")
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("DiscoveryMW::pass_to_successor Stringified serialized buf = {}".format (buf2send))

      #create ZMQ req socket for successor
      req_context = zmq.Context ()  # returns a singleton object
      tmp_req = req_context.socket (zmq.REQ)

      connect_str = "tcp://" + successor['IP'] + ":" + successor['port']
      tmp_req.connect (connect_str)
      self.logger.info ("DiscoveryMW::pass_to_successor successor connected to {}".format(connect_str))

      self.logger.info ("DiscoveryMW::pass_to_successor sending message to successor {}".format(successor[id]))
      # now send this to our discovery service
      identity = f"{self.addr}:{self.port}".encode()
      tmp_req.send_multipart ([identity, buf2send])   # we use the "send" method of ZMQ that sends the bytes
      tmp_req.close()
    except Exception as e:
      raise e

  ########################################
  # Send is ready status back to requester
  ########################################
  def send_is_ready (self, is_ready, ip, port):
    ''' send the is ready status '''
    try:
      self.logger.info ("DiscoveryMW::send_is_ready")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_is_ready - populate the nested IsReady msg")
      isready_resp = discovery_pb2.IsReadyResp ()  # allocate 
      isready_resp.status = is_ready
      
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::send_is_ready - done populating nested IsReady msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::send_is_ready - build the outer DiscoveryResp message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.isready_resp.CopyFrom (isready_resp)
      self.logger.debug ("DiscoveryMW::send_is_ready - done building the outer message")
      
      self.send_message(self.rep, disc_resp, ip, port)
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("DiscoveryMW::send_is_ready - request sent and now wait for reply")
      
    except Exception as e:
      raise e

  ########################################
  # get all pubs from discovery service
  ########################################
  def lookup_all_pubs (self, ip, port):
    ''' register the appln with the discovery service '''

    try:
      self.logger.info ("DiscoveryMW::lookup_all_pubs")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a LookupAllPubsReq message
      self.logger.debug ("DiscoveryMW::lookup_all_pubs - populate the nested LookupAllPubsReq msg")
      allpubs_req = discovery_pb2.LookupAllPubsReq ()  # allocate 
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::lookup_all_pubs - done populating nested LookupAllPubsReq msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::lookup_all_pubs - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.allpubs_req.CopyFrom (allpubs_req)
      self.logger.debug ("DiscoveryMW::lookup_all_pubs - done building the outer message")
      
      self.send_message(self.rep, disc_req, ip, port)
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("DiscoveryMW::lookup_all_pubs - request sent and now wait for reply")
      
    except Exception as e:
      raise e

  ########################################
  # Send list of topic publishers 
  # back to subscriber
  ########################################
  def send_topic_publishers (self, topic_pubs, ip, port):
    ''' send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::send_topic_publishers")
      
      disc_resp = self.get_disc_resp_send_topic_publishers(topic_pubs)
      
      self.send_message(self.rep, disc_resp, ip, port)

      self.logger.info ("DiscoveryMW::send_topic_publishers - Complete")
      
    except Exception as e:
      raise e
    
  ########################################
  # Send list of topic publishers 
  # back to subscriber
  ########################################
  def forward_topic_publishers (self, topic_pubs, ip, port):
    ''' send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::send_topic_publishers")
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_topic_publishers - populate the nested LookupPubByTopicResp msg")
      lookup_resp = discovery_pb2.LookupPubByTopicResp ()  # allocate 

      for p in topic_pubs:
        message_publisher = lookup_resp.publishers.add()
        message_publisher.id = p.id
        message_publisher.addr = p.addr
        message_publisher.port = p.port
        self.logger.debug ("tcp://{}:{}".format(message_publisher.addr, message_publisher.port))

      self.logger.debug ("DiscoveryMW::send_topic_publishers - done prepping message publishers")

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::send_topic_publishers - done populating nested LookupPubByTopicResp msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::send_topic_publishers - build the outer DiscoveryResp message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.lookup_resp.CopyFrom (lookup_resp)
      self.logger.debug ("DiscoveryMW::send_topic_publishers - done building the outer message")
      
      self.send_message(self.rep, disc_resp, ip, port)
      
    except Exception as e:
      raise e
    
  def chord_send_topic_publishers (self, topics, topic_pubs, current_pubs_list, first_node_hash, sender_ip, sender_port, successor):
    ''' chord send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::chord_send_topic_publishers")
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::chord_send_topic_publishers - populate the nested ChordLookupPubByTopicReq msg")
      chord_lookup_req = discovery_pb2.ChordLookupPubByTopicReq ()  # allocate 
      
      chord_lookup_req.topiclist[:] = topics
      chord_lookup_req.first_node_hash = first_node_hash
      chord_lookup_req.sender_ip = sender_ip
      chord_lookup_req.sender_port = sender_port

      for p in topic_pubs:
        message_publisher = chord_lookup_req.publishers.add()
        message_publisher.id = p.name
        message_publisher.addr = p.address
        message_publisher.port = p.port
        self.logger.debug ("tcp://{}:{}".format(message_publisher.addr, message_publisher.port))

      chord_lookup_req.publishers.extend(current_pubs_list)

      self.logger.debug ("DiscoveryMW::chord_send_topic_publishers - done prepping message publishers")

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::chord_send_topic_publishers - done populating nested LookupPubByTopicResp msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::chord_send_topic_publishers - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.CHORD_TYPE_LOOKUP_PUB_BY_TOPIC
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.chord_lookup_req.CopyFrom (chord_lookup_req)
      self.logger.debug ("DiscoveryMW::chord_send_topic_publishers - done building the outer message")
      
      self.pass_to_successor(disc_req, successor)
      
    except Exception as e:
      raise e

  def get_disc_resp_send_all_publishers(self, topic_pubs):
    # first build a IsReady message
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_all_publishers - populate the nested LookupAllPubsResp msg")
      lookup_resp = discovery_pb2.LookupAllPubsResp ()  # allocate 

      for p in topic_pubs:
        message_publisher = lookup_resp.publishers.add()
        message_publisher.id = p.name
        message_publisher.addr = p.address
        message_publisher.port = p.port
        self.logger.debug ("tcp://{}:{}".format(message_publisher.addr, message_publisher.port))

      self.logger.debug ("DiscoveryMW::get_disc_resp_send_all_publishers - done prepping message publishers")

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_all_publishers - done populating nested LookupAllPubsResp msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_all_publishers - build the outer DiscoveryResp message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.allpubs_resp.CopyFrom (lookup_resp)
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_all_publishers - done building the outer message")
      return disc_resp
  
  def get_disc_resp_send_topic_publishers(self, topic_pubs):
      # first build a IsReady message
      self.logger.info ("DiscoveryMW::get_disc_resp_send_topic_publishers")
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_topic_publishers - populate the nested LookupPubByTopicResp msg")
      lookup_resp = discovery_pb2.LookupPubByTopicResp ()  # allocate 

      for p in topic_pubs:
        message_publisher = lookup_resp.publishers.add()
        message_publisher.id = p.name
        message_publisher.addr = p.address
        message_publisher.port = p.port
        self.logger.debug ("tcp://{}:{}".format(message_publisher.addr, message_publisher.port))

      self.logger.debug ("DiscoveryMW::get_disc_resp_send_topic_publishers - done prepping message publishers")

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_topic_publishers - done populating nested LookupPubByTopicResp msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_topic_publishers - build the outer DiscoveryResp message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.lookup_resp.CopyFrom (lookup_resp)
      self.logger.debug ("DiscoveryMW::get_disc_resp_send_topic_publishers - done building the outer message")
      return disc_resp

    ########################################
  # Send list of all publishers 
  # back to broker
  ########################################
  def send_all_publishers (self, topic_pubs, ip, port):
    ''' send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::send_all_publishers")
      
      disc_resp = self.get_disc_resp_send_all_publishers(topic_pubs)
      
      self.send_message(self.rep, disc_resp, ip, port)
      
      self.logger.info ("DiscoveryMW::send_all_publishers completed")
    except Exception as e:
      raise e
    
  def forward_all_publishers (self, topic_pubs, ip, port):
    ''' send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::send_all_publishers")
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_all_publishers - populate the nested LookupAllPubsResp msg")
      lookup_resp = discovery_pb2.LookupAllPubsResp ()  # allocate 

      for p in topic_pubs:
        message_publisher = lookup_resp.publishers.add()
        message_publisher.id = p.id
        message_publisher.addr = p.addr
        message_publisher.port = p.port
        self.logger.debug ("tcp://{}:{}".format(message_publisher.addr, message_publisher.port))

      self.logger.debug ("DiscoveryMW::send_all_publishers - done prepping message publishers")

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::send_all_publishers - done populating nested LookupAllPubsResp msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::send_all_publishers - build the outer DiscoveryResp message")
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.allpubs_resp.CopyFrom (lookup_resp)
      self.logger.debug ("DiscoveryMW::send_all_publishers - done building the outer message")
      
      self.send_message(self.rep, disc_resp, ip, port)
      
    except Exception as e:
      raise e
    
  def chord_send_all_publishers (self, publishers, current_pubs_list, first_node_hash, sender_ip, sender_port, successor):
    ''' chord send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::chord_send_all_publishers")
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::chord_send_all_publishers - populate the nested ChordLookupPubByTopicReq msg")
      chord_lookup_req = discovery_pb2.ChordLookupAllPubsReq ()  # allocate 
      
      chord_lookup_req.first_node_hash = first_node_hash
      chord_lookup_req.sender_ip = sender_ip
      chord_lookup_req.sender_port = sender_port

      for p in publishers:
        message_publisher = chord_lookup_req.publishers.add()
        message_publisher.id = p.name
        message_publisher.addr = p.address
        message_publisher.port = p.port
        self.logger.debug ("tcp://{}:{}".format(message_publisher.addr, message_publisher.port))

      chord_lookup_req.publishers.extend(current_pubs_list)

      self.logger.debug ("DiscoveryMW::chord_send_all_publishers - done prepping message publishers")

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::chord_send_all_publishers - done populating nested LookupPubByTopicResp msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::chord_send_all_publishers - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.CHORD_TYPE_LOOKUP_ALL_PUBS
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.chord_lookup_req.CopyFrom (chord_lookup_req)
      self.logger.debug ("DiscoveryMW::chord_send_all_publishers - done building the outer message")
      
      self.pass_to_successor(disc_req, successor)
      
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