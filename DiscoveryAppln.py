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
    self.is_ready = False
    self.discovery_ledger = None

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
      self.discovery_ledger = DiscoveryLedger()
      
      # Now, get the configuration object
      self.logger.debug ("DiscoveryAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
    
      # Now get our topic list of interest
      self.logger.debug ("DiscoveryAppln::configure - selecting our topic list")
      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("DiscoveryAppln::configure - initialize the middleware object")
      self.mw_obj = DiscoveryMW(self.logger)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object
      
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

  
  ########################################
  # handle register request method called as part of upcall
  #
  # As mentioned in class, the middleware object can do the reading
  # from socket and deserialization. But it does not know the semantics
  # of the message and what should be done. So it becomes the job
  # of the application. Hence this upcall is made to us.
  ########################################
  def register_request (self, reg_req):
    ''' handle register request '''

    try:
      self.logger.info ("DiscoveryAppln::register_request")
      success = False
      reason = ""

      # if publisher, check if publisher is already registered, if not, add to ledger.
      if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
        if (not any(p.name == reg_req.info.id for p in self.discovery_ledger.publishers)):
            self.discovery_ledger.publishers.append(Registrant(reg_req.info.id, reg_req.info.addr, reg_req.info.port, reg_req.topiclist))
            success = True
        else:
            reason = "Publisher names must be unique."
    
      # if subscriber, check if subscriber is already registered, if not, add to ledger.
      elif (reg_req.role == discovery_pb2.ROLE_SUBSCRIBER):
        if (not any(s.name == reg_req.info.id for s in self.discovery_ledger.subscribers)):
            self.discovery_ledger.subscribers.append(Registrant(reg_req.info.id))
            success = True
        else:
            reason = "Publisher names must be unique."
      
      # there should only be subscriber and publisher types requesting to register
      else:
        raise Exception ("Unknown event after poll")

      if (len(self.discovery_ledger.publishers) >= self.pubs and len(self.discovery_ledger.subscribers) >= self.subs):
        self.is_ready = True
    
      self.mw_obj.send_register_status(success, reason)

      # return a timeout of zero so that the event loop in its next iteration will immediately make
      # an upcall to us
      return 0

    except Exception as e:
      raise e

  ########################################
  # handle isready request method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def isready_request (self):
    ''' handle isready request '''

    try:
      self.logger.info ("DiscoveryAppln::isready_request")

      # use middleware to serialize and send the is ready status
      self.mw_obj.send_is_ready(self.is_ready)

      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e


  ########################################
  # handle isready request method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def lookup_by_topic_request (self, lookup_req):
    ''' handle isready request '''

    try:
      self.logger.info ("DiscoveryAppln::lookup_by_topic_request")

      # get list of publishers that match up with any topic in the request topic list
      topic_pubs = [p for p in self.discovery_ledger.publishers if any(t in p.topic_list for t in lookup_req.topiclist)]

      # use middleware to serialize and send the is ready status
      self.mw_obj.send_topic_publishers(topic_pubs)

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
      self.logger.info ("     Dissemination: {}".format (self.dissemination))
      self.logger.info ("     Num Topics: {}".format (self.num_topics))
      self.logger.info ("     TopicList: {}".format (self.topiclist))
      self.logger.info ("     Iterations: {}".format (self.iters))
      self.logger.info ("     Frequency: {}".format (self.frequency))
      self.logger.info ("**********************************")

    except Exception as e:
      raise e

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
  
  parser.add_argument ("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", type=int, default=5577, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")

  parser.add_argument ("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
  
  parser.add_argument ("-s", "--subs", type=int, default=1, help="number of needed subscribers to be ready (default: 1)")

  parser.add_argument ("-p", "--pubs", type=int, default=1, help="number of needed publishers to be ready (default: 1)")


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