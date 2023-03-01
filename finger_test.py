# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose:
#
# This code uses code suggested in
#
#     https://stackoverflow.com/questions/67219691/python-hash-function-that-returns-32-or-64-bits
#
# that retrieves as 32 or 64 bit integer from a hashlib.sha256 hash value (digest). But then, we want
# to know if this will result in any collisions. So we are going to generate a large number of
# registration strings (like what publishers or subscribers would send) as well as the Discovery
# service's string which results in a node number (to be eventually used in Chord)
#
# Instead of 64, we try 48 and at least the multiple runs of this did not show any collisions with
# a 48 bit hash. So this may be an attractive hash function to use for PA2 that is going to use Chord.

import os
import random # random number generation
import hashlib  # for the secure hash library
import argparse # argument parsing
import logging # for logging. Use it in place of print statements.

#needed for Finger operations specifically
import json
import math

class FingerTester ():
  #################
  # constructor
  #################
  def __init__ (self, logger):
    # The following uses the hash value as key and value is the id;
    # if same hash val for another id => collision
    self.dht_nodes = []
    self.perspective = None
    self.logger = logger

  #################
  # configuration
  #################
  def configure (self, args):
    # Here we initialize any internal variables
    self.logger.debug ("FingerTester::configure")

    self.perspective = args.perspective

    self.logger.debug ("FingerTester::Dump")
    self.logger.debug ("\Perspective Hash = {}".format (self.perspective))

  #################
  # Driver program
  #################
  def driver (self):
    self.logger.debug ("CollisionTester::driver")

    # load the dht file into a dictionary object
    with open('dht.json') as json_file:
        self.dht_nodes = json.load(json_file)

    # sort the dict by hash value asc
    self.dht_nodes = sorted(self.dht_nodes, key=lambda d: d['hash']) 

    # get the max hash to know what to modulus for the ring of hashes
    max_hash = self.dht_nodes[-1]['hash'];

    # loop powers of 2 on the hash value to pick the m finger entries?
    finger_table = []
    my_index = [i for i, d in enumerate(self.dht_nodes) if d['hash'] == self.perspective][0]
    for i in range(self.dht_nodes.count()):
      finger_table.append(self.dht_nodes[self.dht_nodes.count() % (i + my_index)])
      
    for finger in finger_table:
      self.logger.debug ("CollisionTester::driver finger: {}".format(json.dump(finger)))
    
      
###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="FingerTester")
  
  # Now specify all the optional arguments we support
  #
  # Specify number of bits of hash to test

  parser.add_argument ("-p", "--perspective", default="215846852735421", help="Number of iterations, default 1 million")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")
  
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
    logger = logging.getLogger ("FingerTest")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain the test object
    logger.debug ("Main: obtain the FingerTester object")
    test_obj = FingerTester (logger)

    # configure the object
    logger.debug ("Main: configure the test object")
    test_obj.configure (args)

    # now invoke the driver program
    logger.debug ("Main: invoke the test obj driver")
    test_obj.driver ()

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