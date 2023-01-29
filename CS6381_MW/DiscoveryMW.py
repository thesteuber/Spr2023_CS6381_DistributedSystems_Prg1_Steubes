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
    self.rep = None # will be a ZMQ REP socket to talk to Discovery service
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.upcall_obj = None # handle to appln obj to handle appln-specific data
    self.handle_events = True # in general we keep going thru the event loop

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("DiscoveryMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      
      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("DiscoveryMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the REP socket
      # REP is needed because we are the responder of the Discovery service
      self.logger.debug ("DiscoveryMW::configure - obtain REP sockets")
      self.rep = context.socket (zmq.REP)

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
      bytesRcvd = self.rep.recv()

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here DiscoveryResp and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_req = discovery_pb2.DiscoveryReq()
      disc_req.ParseFromString(bytesRcvd)

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so. See how we make
      # the upcall on the application object by using the saved handle to the appln object.
      #
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.
      if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
        # let the appln level object decide what to do
        timeout = self.upcall_obj.register_request (disc_req.register_req)
      elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is the is ready request
        timeout = self.upcall_obj.isready_request()
      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # this is a lookup publishers by topic/s request
        timeout = self.upcall_obj.lookup_by_topic_request(disc_req.lookup_req)

      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError ("Unrecognized response message")

      return timeout
    
    except Exception as e:
      raise e


  ########################################
  # Send is register status back to requester
  ########################################
  def send_register_status (self, success, reason):
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
      reg_resp.status = discovery_pb2.Status.STATUS_SUCCESS if success else discovery_pb2.Status.STATUS_FAILURE
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
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()

      # now send this to our discovery service
      self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
    except Exception as e:
      raise e


  ########################################
  # Send is ready status back to requester
  ########################################
  def send_is_ready (self, is_ready):
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
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()

      # now send this to our discovery service
      self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("DiscoveryMW::send_is_ready - request sent and now wait for reply")
      
    except Exception as e:
      raise e

  ########################################
  # Send list of topic publishers 
  # back to subscriber
  ########################################
  def send_topic_publishers (self, topic_pubs):
    ''' send topic publishers '''
    try:
      self.logger.info ("DiscoveryMW::send_topic_publishers")
      
      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::send_topic_publishers - populate the nested LookupPubByTopicResp msg")
      lookup_resp = discovery_pb2.LookupPubByTopicResp ()  # allocate 

      for p in topic_pubs:
        message_publisher = lookup_resp.publishers.add()
        message_publisher.id = p.name
        message_publisher.addr = p.address
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
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()

      # now send this to our discovery service
      self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
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