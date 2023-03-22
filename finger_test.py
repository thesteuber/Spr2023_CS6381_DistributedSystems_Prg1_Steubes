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
    self.bits_hash = None
    self.logger = logger

  #################
  # configuration
  #################
  def configure (self, args):
    # Here we initialize any internal variables
    self.logger.debug ("FingerTester::configure")

    self.perspective = args.perspective
    self.bits_hash = args.bits_hash

    self.logger.debug ("FingerTester::Dump")
    self.logger.debug ("\Perspective Hash = {}".format (self.perspective))

  def create_finger_table(self, my_index, nodes):
    m = self.bits_hash
    finger_table = []
    max_hash = nodes[-1]['hash']
    for i in range(m):
        next_index = (my_index + 2**i) % m
        finger = self.find_successor(next_index, nodes)
        finger_table.append(finger)
    return finger_table

  def find_successor(self, index, nodes):
    hash_at_index = nodes[index]['hash']
    m = self.bits_hash
    for i in range(m):
        if nodes[i]['hash'] >= hash_at_index:
            return nodes[i]
    return nodes[0]

  def hash_func (self, id):
    self.logger.debug ("ExperimentGenerator::hash_func")

    # first get the digest from hashlib and then take the desired number of bytes from the
    # lower end of the 256 bits hash. Big or little endian does not matter.
    hash_digest = hashlib.sha256 (bytes (id, "utf-8")).digest ()  # this is how we get the digest or hash value
    # figure out how many bytes to retrieve
    num_bytes = int(self.bits_hash/8)  # otherwise we get float which we cannot use below
    hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes

    return hash_val

  #################
  # Driver program
  #################
  def driver (self):
    self.logger.debug ("CollisionTester::driver")

    # load the dht file into a dictionary object
    with open('dht.json') as json_file:
        self.dht_nodes = json.load(json_file).get('dht')

    # sort the dict by hash value asc
    self.dht_nodes = sorted(self.dht_nodes, key=lambda d: d.get('hash', None)) 

    # get the max hash to know what to modulus for the ring of hashes
    max_hash = self.dht_nodes[-1]['hash'];

    # loop powers of 2 on the hash value to pick the m finger entries?
    finger_table = []
    my_index = [i for i, d in enumerate(self.dht_nodes) if d['hash'] == self.perspective][0]
    
    if self.dht_nodes[my_index]['port']:
      string = self.dht_nodes[my_index]['id'] + ":" + self.dht_nodes[my_index]['IP'] + ":" + str (self.dht_nodes[my_index]['port'])  # will be the case for disc and pubs
    else:
      string = self.dht_nodes[my_index]['id'] + ":" + self.dht_nodes[my_index]['IP']  # will be the case for subscribers

    finger_table = self.create_finger_table(my_index, self.dht_nodes)

    # now get the hash value for this string
    hash_val_gen = self.hash_func (string)
    hash_val_already = self.dht_nodes[my_index]['hash']

    self.logger.debug ("My already hash: {}".format(hash_val_already))
    self.logger.debug ("My generated hash: {}".format(hash_val_gen))
    
    # self.logger.debug ("CollisionTester::driver my hash index: {}".format(str(my_index)))
    # for i in range(len(self.dht_nodes))[1:]:
    #   next_index = i + my_index
    #   if next_index >= len(self.dht_nodes):
    #     next_index = next_index - len(self.dht_nodes) 
      
    #   self.logger.debug ("CollisionTester::driver next index: {}".format(str(next_index)))
    #   finger_table.append(self.dht_nodes[next_index])

    # self.logger.debug ("Sorted Nodes")
    # for node in self.dht_nodes:
    #   self.logger.debug ("{}".format(node))

    # self.logger.debug ("Finger Table")
    # for finger in finger_table:
    #   self.logger.debug ("{}".format(finger))
    

      
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

  parser.add_argument ("-p", "--perspective", type=int, default=215846852735421, help="perspective hash to build finger table")
  parser.add_argument ("-b", "--bits_hash", type=int, choices=[8,16,24,32,40,48,56,64], default=48, help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48")
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