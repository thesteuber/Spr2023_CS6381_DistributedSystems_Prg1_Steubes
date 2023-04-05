###############################################
#
# Author: Sean Steuber
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in
from DiscoveryLedger import DiscoveryLedger, Registrant
from ManagerAdapter import ManagerAdapter
import json
import hashlib  # for the secure hash library

##################################
#       DiscoveryAppln class
##################################
class DiscoveryAppln ():

  # these are the states through which our publisher appln object goes thru.
  # We maintain the state so we know where we are in the lifecycle and then
  # take decisions accordingly
  class State (Enum):
    INITIALIZE = 0,
    CONFIGURE = 1,
    REGISTER = 2,
    ISREADY = 3,
    DISSEMINATE = 4,
    COMPLETED = 5

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.state = self.State.INITIALIZE # state that are we in
    self.name = None # our name (some unique name)
    self.iters = None   # number of iterations of publication
    self.frequency = None # rate at which dissemination takes place
    self.num_topics = None # total num of topics we publish
    self.lookup = None # one of the diff ways we do lookup
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.subs = None # expected number of subscribers before ready
    self.pubs = None # expected number of publishers before ready
    self.is_ready = True # now true as the concept of not being ready is no longer
    self.discovery_ledger = None
    self.dissemination = None # Method by which messages are disseminated: ViaBroker or Direct
    self.dht_nodes = [] # For all DHT nodes in dht.json file
    self.node_hash = None # Hash value of this DHT Node
    self.previous_node_hash = None # Hash value of the DHT node that precedes this one
    self.bits_hash = None # num bits for the hashing
    self.finger_table = None # Finger table of nearby nodes if lookup is Chord
    self.dht_json = None # Location of dht nodes json file for Chord
    self.distributed_topics = [] # list of topics that this DHT node will store in Chord
    self.chord_pubs_registered = 0 # count of pubs registered across DHT ring while in Chord discovery
    self.chord_subs_registered = 0 # count of subs registered across DHT ring while in Chord discovery
    self.adapter = None # Zookeeper Discovery Service Adapater

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("DiscoveryAppln::configure")

      # set our current state to CONFIGURE state
      self.state = self.State.CONFIGURE
      
      # initialize our variables
      self.name = args.name # our name
      self.iters = args.iters  # num of iterations
      self.frequency = args.frequency # frequency with which topics are disseminated
      self.num_topics = args.num_topics  # total num of topics we publish
      self.subs = args.subs
      self.pubs = args.pubs
      self.bits_hash = args.bits_hash
      self.dht_json = args.dht_json
      self.discovery_ledger = DiscoveryLedger()
      self.zoo_host = args.zookeeper
      self.adapter = ManagerAdapter(self.zoo_host, self.logger)
      

      # Now, get the configuration object
      self.logger.debug ("DiscoveryAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
      
      # load the dht file into a dictionary object
      if (self.lookup == "Chord"):
        #configure the discovery app for chord
        self.configure_chord()

      # Now get our topic list of interest
      self.logger.debug ("DiscoveryAppln::configure - selecting our topic list")
      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("DiscoveryAppln::configure - initialize the middleware object")
      self.mw_obj = DiscoveryMW(self.logger)
      self.mw_obj.configure (args, self.lookup) # pass remainder of the args to the m/w object
      self.adapter.set_bleader_callback_handle(self.mw_obj.broker_leader_handle)
      self.adapter.set_dleader_callback_handle(self.refresh_discovery_with_zoo)
      
      self.logger.info ("DiscoveryAppln::configure - configuration complete")
      
    except Exception as e:
      raise e

  

  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program '''

    try:
      self.logger.info ("DiscoveryAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()

      # First ask our middleware to keep a handle to us to make upcalls.
      # This is related to upcalls. By passing a pointer to ourselves, the
      # middleware will keep track of it and any time something must
      # be handled by the application level, invoke an upcall.
      self.logger.debug ("DiscoveryAppln::driver - upcall handle")
      self.mw_obj.set_upcall_handle (self)

      # Now simply let the underlying middleware object enter the event loop
      # to handle events. However, a trick we play here is that we provide a timeout
      # of zero so that control is immediately sent back to us where we can then
      # register with the discovery service and then pass control back to the event loop
      #
      # As a rule, whenever we expect a reply from remote entity, we set timeout to
      # None or some large value, but if we want to send a request ourselves right away,
      # we set timeout is zero.
      #
      self.mw_obj.event_loop (timeout=0)  # start the event loop
      
      self.logger.info ("DiscoveryAppln::driver completed")
      
    except Exception as e:
      raise e

  def refresh_discovery_with_zoo(self, ip, port):
    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo")

    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo - gather pubs")
    publishers = self.adapter.get_publishers()

    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo - gather subs")
    subscribers = self.adapter.get_subscribers()

    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo - gather primary broker")
    broker = self.adapter.get_primary_broker_registrant()

    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo - gathering complete")
    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo - create ledger")
    new_ledger = DiscoveryLedger()
    new_ledger.publishers = publishers
    new_ledger.subscribers = subscribers
    new_ledger.broker = broker

    self.discovery_ledger = new_ledger

    self.logger.info ("DiscoveryAppln::refresh_discovery_with_zoo - Completed")

  def reg_single_publisher(self, reg_req):
    self.logger.info ("DiscoveryAppln::reg_single_publisher")
    success = False
    reason = ""
    registrant = Registrant(reg_req.info.id, reg_req.info.addr, reg_req.info.port, reg_req.topiclist)
    self.adapter.register_publisher(registrant)
    if (not any(p.name == registrant.name for p in self.discovery_ledger.publishers)):
        self.discovery_ledger.publishers.append(registrant)
        self.refresh_leading_broker_publishers()
        if (self.dissemination != "Broker"):
          self.refresh_subscribers_publishers()
        success = True
    else:
        reason = "Publisher names must be unique."
    
    return success, reason
  
  def reg_single_subscriber(self, reg_req):
    self.logger.info ("DiscoveryAppln::reg_single_subscriber")
    success = False
    reason = ""
    registrant = Registrant(reg_req.info.id, reg_req.info.addr, reg_req.info.port, reg_req.topiclist)
    self.adapter.register_subscriber(registrant)
    if (not any(s.name == registrant.name for s in self.discovery_ledger.subscribers)):
        self.discovery_ledger.subscribers.append(registrant)
        self.mw_obj.add_sub_req_socket(registrant)
        success = True
    else:
        reason = "Subscriber names must be unique."
    
    return success, reason
  
  def reg_single_broker(self, reg_req):
    self.logger.info ("DiscoveryAppln::reg_single_broker")
    success = False
    reason = ""
    registrant = Registrant(reg_req.info.id, reg_req.info.addr, reg_req.info.port, reg_req.topiclist)
    #self.adapter.register_subscriber(registrant)
    if (self.discovery_ledger.broker == None):
      self.discovery_ledger.broker = registrant
      self.mw_obj.set_broker_leader(registrant)
      
    success = True
    
    return success, reason


  ########################################
  # handle register request method called as part of upcall
  #
  # As mentioned in class, the middleware object can do the reading
  # from socket and deserialization. But it does not know the semantics
  # of the message and what should be done. So it becomes the job
  # of the application. Hence this upcall is made to us.
  ########################################
  def register_request (self, reg_req, ip, port):
    ''' handle register request '''

    try:
      self.logger.info ("DiscoveryAppln::register_request")
      success = False
      reason = ""

      # if publisher, check if publisher is already registered, if not, add to ledger.
      if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
        if (self.lookup == "Chord"):
          success, reason = self.chord_register_publisher(reg_req)
        else:
          success, reason = self.reg_single_publisher(reg_req)
    
      # if subscriber, check if subscriber is already registered, if not, add to ledger.
      elif (reg_req.role == discovery_pb2.ROLE_SUBSCRIBER):
        if (self.lookup == "Chord"):
          success, reason = self.chord_register_subscriber(reg_req)
        else:
          success, reason = self.reg_single_subscriber(reg_req)

      # if broker, check if broker is already registered, if not, add to ledger.
      elif (reg_req.role == discovery_pb2.ROLE_BOTH):
        if (self.lookup == "Chord"):
          self.logger.info ("DiscoveryAppln::register_request CHORD")
          self.chord_set_broker(self.mw_obj.get_set_broker_req(self.node_hash, reg_req.info))
        else:
          success, reason = self.reg_single_broker(reg_req)
      
      # there should only be subscriber and publisher types requesting to register
      else:
        raise Exception ("Unknown event after poll")

      if (self.lookup == "Chord"):
        self.logger.info ("DiscoveryAppln::register_request incrementing for chord happens in chord_register_publisher and chord_register_subscriber")
      else:
        if (len(self.discovery_ledger.publishers) >= self.pubs and len(self.discovery_ledger.subscribers) >= self.subs):
          if (self.dissemination == "Direct" or (self.dissemination == "Broker" and self.discovery_ledger.broker != None)):
            self.is_ready = True
    
      if (self.lookup != "Chord"):
        self.mw_obj.send_register_status(success, reason, ip, port)
        

      # return a timeout of zero so that the event loop in its next iteration will immediately make
      # an upcall to us
      return 0

    except Exception as e:
      raise e

  def unregister_request (self, reg_req, ip, port):
    ''' handle unregister request '''

    try:
      self.logger.info ("DiscoveryAppln::unregister_request")
      success = False
      reason = ""
      registrant = Registrant(reg_req.info.id, reg_req.info.addr, reg_req.info.port, reg_req.topiclist)

      if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
        self.adapter.unregister_publisher(registrant)
      elif (reg_req.role == discovery_pb2.ROLE_SUBSCRIBER):
        self.adapter.unregister_subscriber(registrant)
        self.mw_obj.remove_sub_req_socket(registrant)

      self.discovery_ledger.remove_registrant(reg_req.info.id)
      success = True
    
      self.mw_obj.send_unregister_status(success, reason, ip, port)

      # return a timeout of zero so that the event loop in its next iteration will immediately make
      # an upcall to us
      return 0

    except Exception as e:
      raise e

  def refresh_leading_broker_publishers(self):
    if self.discovery_ledger.broker != None:
      self.logger.info ("DiscoverlyMw::refresh_broker_publishers - send all")
      mylist = self.mw_obj.get_disc_resp_send_all_publishers(self.discovery_ledger.publishers)
      self.mw_obj.send_message(self.mw_obj.broker_req_socket, mylist, None, None)
      self.logger.info ("DiscoverlyMw::refresh_broker_publishers - send all completed")

  def refresh_subscribers_publishers(self):
    self.logger.info ("DiscoverlyMw::refresh_subscribers_publishers")
    if (self.dissemination != "Broker"):
      mylist = self.mw_obj.get_disc_resp_send_topic_publishers(self.discovery_ledger.publishers)
      self.mw_obj.refresh_subscribers_publishers(self.mw_obj.broker_req_socket, mylist)
    else:
      mylist = self.mw_obj.get_disc_resp_send_topic_publishers([self.discovery_ledger.broker])
      self.mw_obj.refresh_subscribers_publishers(self.mw_obj.broker_req_socket, mylist)
    self.logger.info ("DiscoverlyMw::refresh_subscribers_publishers - completed")


  ########################################
  # handle isready request method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def isready_request (self, ip, port):
    ''' handle isready request '''

    try:
      self.logger.info ("DiscoveryAppln::isready_request")

      # use middleware to serialize and send the is ready status
      self.mw_obj.send_is_ready(self.is_ready, ip, port)

      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e


  ########################################
  # handle lookup_by_topic_request request method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def lookup_by_topic_request (self, lookup_req, sender_ip, sender_port):
    ''' handle isready request '''

    try:
      self.logger.info ("DiscoveryAppln::lookup_by_topic_request")

      # get list of publishers that match up with any topic in the request topic list
      topic_pubs = None
      if (self.dissemination == "Broker"):
        if self.discovery_ledger.broker == None:
          topic_pubs = []
        else:
          topic_pubs = [self.discovery_ledger.broker] # only send the broker to subs requesting pubs if via broker
      else:
        topic_pubs = [p for p in self.discovery_ledger.publishers if any(t in p.topic_list for t in lookup_req.topiclist)]

      if (self.lookup == "Chord"):
        self.logger.info ("DiscoveryAppln::lookup_by_topic_request chord lookup")
        self.mw_obj.chord_send_topic_publishers(lookup_req.topiclist, topic_pubs, [], self.node_hash, sender_ip, sender_port, self.finger_table[0])
      else:
        self.logger.info ("DiscoveryAppln::lookup_by_topic_request normal lookup")
        # use middleware to serialize and send the is ready status
        self.mw_obj.send_topic_publishers(topic_pubs, sender_ip, sender_port)

      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e

    ########################################
  # handle lookup_all_pubs request method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def lookup_all_pubs (self, ip, port):
    ''' handle isready request '''

    try:
      self.logger.info ("DiscoveryAppln::lookup_all_pubs")

      if (self.lookup == "Chord"):
        self.logger.info ("DiscoveryAppln::lookup_all_pubs chord lookup")
        self.mw_obj.chord_send_all_publishers(self.discovery_ledger.publishers, [], self.node_hash, ip, port, self.finger_table[0])
      else:
        self.logger.info ("DiscoveryAppln::lookup_all_pubs normal lookup")
        # use middleware to serialize and send the is ready status
        self.mw_obj.send_all_publishers(self.discovery_ledger.publishers, ip, port)

      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e

  ########################################
  # dump the contents of the object 
  ########################################
  def dump (self):
    ''' Pretty print '''

    try:
      self.logger.info ("**********************************")
      self.logger.info ("DiscoveryAppln::dump")
      self.logger.info ("------------------------------")
      self.logger.info ("     Name: {}".format (self.name))
      self.logger.info ("     Lookup: {}".format (self.lookup))
      self.logger.info ("     Num Topics: {}".format (self.num_topics))
      self.logger.info ("     Frequency: {}".format (self.frequency))
      self.logger.info ("**********************************")

    except Exception as e:
      raise e












def configure_chord(self):
  # load in DHT Nodes from json file
  with open(self.dht_json) as json_file:
      self.dht_nodes = json.load(json_file).get('dht')

  # sort the dict by hash value asc
  self.dht_nodes = sorted(self.dht_nodes, key=lambda d: d.get('hash', None))

  # get index of this DHT node
  my_index = [i for i, d in enumerate(self.dht_nodes) if d['id'] == self.name][0]

  # create and store the finger table for this DHT node
  self.finger_table = self.create_finger_table(my_index, self.dht_nodes)

  # set this DHT hash and the predecessor
  self.node_hash = self.dht_nodes[my_index]['hash']
  prev_index = my_index - 1
  if prev_index < 0:
    prev_index = -1
  self.previous_node_hash = self.dht_nodes[prev_index]['hash']

def create_finger_table(self, my_index, nodes):
  m = len(nodes)
  finger_table = []
  
  for i in range(m):
      next_index = (my_index + 2**i) % m
      finger = self.find_successor(nodes[next_index]['hash'], nodes)
      finger_table.append(finger)
  return finger_table

def find_successor(self, key_hash, nodes):
  m = len(nodes)
  for i in range(m):
      if nodes[i]['hash'] >= key_hash:
          return nodes[i]
  return nodes[0]

def find_successor_finger(self, key_hash):
  m = len(self.finger_table)
  for i in range(m):
      if self.finger_table[i]['hash'] >= key_hash:
          return self.finger_table[i]
  return self.finger_table[0]

def hash_func (self, id, ip, port):
  self.logger.debug ("DiscoveryAppln::hash_func")

  key = ""
  if port:
    key = id + ":" + ip + ":" + str (port)  # will be the case for disc and pubs
  else:
    key = id + ":" + ip  # will be the case for subscribers

  self.logger.debug ("DiscoveryAppln::hash_func key to hash: {}".format(key))

  # first get the digest from hashlib and then take the desired number of bytes from the
  # lower end of the 256 bits hash. Big or little endian does not matter.
  hash_digest = hashlib.sha256 (bytes (key, "utf-8")).digest ()  # this is how we get the digest or hash value
  # figure out how many bytes to retrieve
  num_bytes = int(self.bits_hash/8)  # otherwise we get float which we cannot use below
  hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes

  self.logger.debug ("DiscoveryAppln::hash_func hash to return: {}".format(hash_val))
  return hash_val

def pass_disc_req_on_until_reaches_sender(self, disc_req, sender_hash):
  self.logger.info ("DiscoveryAppln::pass_disc_req_on_until_reaches_sender")
  if (self.finger_table[0]['hash'] == sender_hash):
    self.logger.info ("DiscoveryAppln::pass_disc_req_on_until_reaches_sender next is the sender, do nothing")
    return
  
  # use middleware to serialize and pass on the discovery request
  self.logger.info ("DiscoveryAppln::pass_disc_req_on_until_reaches_sender past to successor")
  self.mw_obj.pass_to_successor(disc_req, self.finger_table[0])

def chord_lookup_all_pubs (self, chord_allpubs_req):
  ''' handle isready request '''

  try:
    self.logger.info ("DiscoveryAppln::chord_lookup_all_pubs")

    # if we have made 1 full rotation around the ring, send the gathered publishers to the subscriber
    if (chord_allpubs_req.first_node_hash == self.node_hash):
      self.logger.info ("DiscoveryAppln::chord_lookup_all_pubs sending publishers found on ring to original requester")
      self.mw_ojb.forward_topic_publishers(chord_allpubs_req.publishers, chord_allpubs_req.sender_ip, chord_allpubs_req.sender_port)
      return 0

    self.mw_obj.chord_send_all_publishers(self.discovery_ledger.publishers, chord_allpubs_req.publishers, 
                                          chord_allpubs_req.first_node_hash, chord_allpubs_req.sender_ip, 
                                          chord_allpubs_req.sender_port, self.finger_table[0])

    # return timeout of 0 so event loop calls us back in the invoke_operation
    # method, where we take action based on what state we are in.
    return 0
  
  except Exception as e:
    raise e

def chord_set_broker(self, disc_req):
  self.logger.info ("DiscoveryAppln::chord_set_broker")
  
  if (self.discovery_ledger.broker == None):
    self.discovery_ledger.broker = Registrant(disc_req.chord_setbroker_req.info.id, 
                                              disc_req.chord_setbroker_req.info.addr, 
                                              disc_req.chord_setbroker_req.info.port, 
                                              None)
    
  if (self.finger_table[0]['hash'] == disc_req.chord_setbroker_req.sender_hash):
    self.mw_obj.send_register_status(True, "", 
                                      disc_req.chord_setbroker_req.info.addr, 
                                      disc_req.chord_setbroker_req.info.port)
    return

  self.logger.info ("DiscoveryAppln::chord_set_broker count incremented")

  self.pass_disc_req_on_until_reaches_sender(disc_req, disc_req.chord_setbroker_req.sender_hash)

########################################
  # handle lookup_by_topic_request request method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def chord_lookup_by_topic_request (self, chord_lookup_req):
    ''' handle isready request '''

    try:
      self.logger.info ("DiscoveryAppln::chord_lookup_by_topic_request")
      
      # if we have made 1 full rotation around the ring, send the gathered publishers to the subscriber
      if (chord_lookup_req.first_node_hash == self.node_hash):
        self.logger.info ("DiscoveryAppln::chord_lookup_by_topic_request sending publishers found on ring to original requester")
        self.mw_ojb.forward_topic_publishers(chord_lookup_req.publishers, chord_lookup_req.sender_ip, chord_lookup_req.sender_port)
        return 0
      
      # otherwise keep going around the ring
      # get list of publishers that match up with any topic in the request topic list
      topic_pubs = None
      if (self.dissemination == "Broker"):
        topic_pubs = [self.discovery_ledger.broker] # only send the broker to subs requesting pubs if via broker
      else:
        topic_pubs = [p for p in self.discovery_ledger.publishers if any(t in p.topic_list for t in chord_lookup_req.topiclist)]

      # use middleware to serialize and send the is ready status
      self.mw_obj.chord_send_topic_publishers(chord_lookup_req.topiclist, topic_pubs, chord_lookup_req.publishers,
                                              chord_lookup_req.first_node_hash, chord_lookup_req.sender_ip, 
                                              chord_lookup_req.sender_port, self.finger_table[0])

      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e

  def chord_forward_publisher_register_req(self, reg_req, pub_hash):
    self.logger.info ("DiscoveryAppln::chord_forward_publisher_register_req")
    #get the hash for the DHT node to send the register req to
    send_to_node = None
    for finger in self.finger_table:
      if finger['hash'] > pub_hash:
        break
      else:
        send_to_node = finger # tracking the predecessor

    self.logger.info ("DiscoveryAppln::chord_forward_publisher_register_req send to node {}".format(send_to_node['id']))
    self.mw_obj.forward_register_req_to_node(reg_req, send_to_node)

    self.logger.info ("DiscoveryAppln::chord_forward_publisher_register_req done")

  def chord_forward_subscriber_register_req(self, reg_req, sub_hash):
    self.logger.info ("DiscoveryAppln::chord_forward_subscriber_register_req")
    #get the hash for the DHT node to send the register req to
    send_to_node = None
    for finger in self.finger_table:
      if finger['hash'] > sub_hash:
        break
      else:
        send_to_node = finger # tracking the predecessor

    self.logger.info ("DiscoveryAppln::chord_forward_subscriber_register_req send to node {}".format(send_to_node['id']))
    self.mw_obj.forward_register_req_to_node(reg_req, send_to_node)

    self.logger.info ("DiscoveryAppln::chord_forward_subscriber_register_req done")
    
  def chord_register_publisher(self, reg_req):
    self.logger.info ("DiscoveryAppln::chord_register_publisher")

    # get publisher hash
    self.logger.info ("DiscoveryAppln::chord_register_publisher gathering hashes")
    pub_hash = self.hash_func(reg_req.info.id, reg_req.info.addr, reg_req.info.port)
    my_hash = self.node_hash
    next_hash = self.finger_table[0]['hash']

    success = False
    reason = ""

    # if the pubs hash is greater than mine and I am the last node in the ring
    # ie the next_hash is less than my hash, then register the publisher with me
    if (pub_hash > my_hash and my_hash > next_hash):
      self.logger.info ("DiscoveryAppln::chord_register_publisher register with me the last node in the dht ring.")
      success, reason = self.reg_single_publisher(reg_req)
      self.mw_obj.send_register_status_to(success, reason, reg_req.info.addr, reg_req.info.port)
    
    # if pub_hash is greater than my hash but less than the next hash, register with me
    elif (pub_hash > my_hash and pub_hash <= next_hash):
      self.logger.info ("DiscoveryAppln::chord_register_publisher register with me.")
      success, reason = self.reg_single_publisher(reg_req)
      self.mw_obj.send_register_status_to(success, reason, reg_req.info.addr, reg_req.info.port)

    # Not registering the publisher with me, must send the register request forward to the next 
    # finger in my finger table that is the predecessor of the first finger with a higher hash
    else:
      self.logger.info ("DiscoveryAppln::chord_register_publisher pass the register request forward.")
      self.chord_forward_publisher_register_req(reg_req, pub_hash)

    if success:
      disc_req_for_incrementing = self.mw_obj.get_increment_pub_req(my_hash)
      self.increment_registered_pubs(disc_req_for_incrementing)

    return success, reason
  
  def chord_register_subscriber(self, reg_req):
    self.logger.info ("DiscoveryAppln::chord_register_subscriber")

    # get publisher hash
    self.logger.info ("DiscoveryAppln::chord_register_subscriber gathering hashes")
    sub_hash = self.hash_func(reg_req.info.id, reg_req.info.addr, reg_req.info.port)
    my_hash = self.node_hash
    next_hash = self.finger_table[0]['hash']

    success = False
    reason = ""

    # if the subs hash is greater than mine and I am the last node in the ring
    # ie the next_hash is less than my hash, then register the publisher with me
    if (sub_hash > my_hash and my_hash > next_hash):
      self.logger.info ("DiscoveryAppln::chord_register_subscriber register with me the last node in the dht ring.")
      success, reason = self.reg_single_subscriber(reg_req)
      self.mw_obj.send_register_status(success, reason, reg_req.info.addr, reg_req.info.port)
    
    # if sub_hash is greater than my hash but less than the next hash, register with me
    elif (sub_hash > my_hash and sub_hash <= next_hash):
      self.logger.info ("DiscoveryAppln::chord_register_subscriber register with me.")
      success, reason = self.reg_single_subscriber(reg_req)
      self.mw_obj.send_register_status(success, reason, reg_req.info.addr, reg_req.info.port)

    # Not registering the publisher with me, must send the register request forward to the next 
    # finger in my finger table that is the predecessor of the first finger with a higher hash
    else:
      self.logger.info ("DiscoveryAppln::chord_register_subscriber pass the register request forward.")
      self.chord_forward_subscriber_register_req(reg_req, sub_hash)

    if success:
      disc_req_for_incrementing = self.mw_obj.get_increment_sub_req(my_hash)
      self.increment_registered_subs(disc_req_for_incrementing)

    return success, reason
    

  def increment_registered_pubs (self, disc_req):
    self.logger.info ("DiscoveryAppln::increment_registered_pubs")
    self.chord_pubs_registered = self.chord_pubs_registered + 1
    if (self.chord_pubs_registered >= self.pubs):
      self.is_ready = True
    self.logger.info ("DiscoveryAppln::increment_registered_pubs count incremented")

    self.pass_disc_req_on_until_reaches_sender(disc_req, disc_req.incregpubs_req.sender_hash)

  def increment_registered_subs (self, disc_req):
    self.logger.info ("DiscoveryAppln::increment_registered_subs")
    self.chord_subs_registered = self.chord_subs_registered + 1
    if (self.chord_subs_registered >= self.subs):
      self.is_ready = True
    self.logger.info ("DiscoveryAppln::increment_registered_subs count incremented.")

    self.pass_disc_req_on_until_reaches_sender(disc_req, disc_req.incregsubs_req.sender_hash)



###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="Discovery Application")
  
  # Now specify all the optional arguments we support
  # At a minimum, you will need a way to specify the IP and port of the lookup
  # service, the role we are playing, what dissemination approach are we
  # using, what is our endpoint (i.e., port where we are going to bind at the
  # ZMQ level)
  
  parser.add_argument ("-n", "--name", default="disc", help="Some name assigned to us. Keep it unique per publisher")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", type=int, default=5555, help="Port number on which our underlying publisher ZMQ service runs, default=5555")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")

  parser.add_argument ("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
  
  parser.add_argument ("-s", "--subs", type=int, default=1, help="number of needed subscribers to be ready (default: 1)")

  parser.add_argument ("-P", "--pubs", type=int, default=1, help="number of needed publishers to be ready (default: 1)")

  parser.add_argument ("-b", "--bits_hash", type=int, choices=[8,16,24,32,40,48,56,64], default=48, help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48")
  
  parser.add_argument ("-j", "--dht_json", default="dht.json", help="Location of dht nodes json file.")
  
  parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IP Addr:Port combo for the zookeeper server, default localhost:2181")


  return parser.parse_args()


###################################
#
# Main program
#
###################################
def main ():
  try:
    # obtain a system wide logger and initialize it to debug level to begin with
    logging.info ("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger ("DiscoveryAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a publisher application
    logger.debug ("Main: obtain the publisher appln object")
    disc_app = DiscoveryAppln(logger)

    # configure the object
    logger.debug ("Main: configure the publisher appln object")
    disc_app.configure (args)

    # now invoke the driver program
    logger.debug ("Main: invoke the publisher appln driver")
    disc_app.driver ()

  except Exception as e:
    logger.error ("Exception caught in main - {}".format (e))
    return

    
###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


  main ()