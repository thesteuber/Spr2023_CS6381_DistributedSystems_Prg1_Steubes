###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose:
# This is an example showing a list of pre-defined topics from which publishers
# and subscribers can choose which ones they would like to use for publication
# and subscription, respectively.
#
# To be used by a publisher or subscriber application logic only. See their code
#
# Created: Spring 2023
#
###############################################

# since we are going to publish or subscribe to a random sampling of topics,
# we need this package
import random
import hashlib  # for the secure hash library

# define a helper class to hold all the topics that we support in our system
class TopicSelector ():
  
  # some pre-defined topics from which a publisher or subscriber chooses
  # from. Feel free to extend it or completely change these. All up to you.
  # I am providing some initial starter capabilitiy.
  #
  # Say these are a list of all topics that are published.
  topiclist = ["weather", "humidity", "airquality", "light", \
                          "pressure", "temperature", "sound", "altitude", \
                          "location"]

  # return a random subset of topics from this list, which becomes our interest
  # A publisher or subscriber application logic will invoke this method to get their
  # interest. 
  def interest (self, num=1):
    # here we just randomly create a subset from this list and return it
    #return random.sample (self.topiclist, random.randint (1, len (self.topiclist)))
    return random.sample (self.topiclist, num)

  def hash_func(self, value, bits_hash):
    # first get the digest from hashlib and then take the desired number of bytes from the
    # lower end of the 256 bits hash. Big or little endian does not matter.
    hash_digest = hashlib.sha256 (bytes (value, "utf-8")).digest ()  # this is how we get the digest or hash value
    # figure out how many bytes to retrieve
    num_bytes = int(bits_hash/8)  # otherwise we get float which we cannot use below
    hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes

    return hash_val

  def get_hashed_pairs(self, bits_hash):
    hashed_topics = []

    for topic in self.topiclist:
      hashed_topics.append({"key" : self.hash_func(topic, bits_hash), "value" : topic})

    return hashed_topics

  # generate a publication on a given topic
  def gen_publication (self, topic):
    if (topic == "weather"):
      return random.choice (["sunny", "cloudy", "rainy", "foggy", "icy"])
    elif (topic == "humidity"):
      return str (random.uniform (10.0, 100.0))
    elif (topic == "airquality"):
      return random.choice (["good", "smog", "poor"])
    elif (topic == "light"):
      # in lumens
      return random.choice (["450", "800", "1100", "1600"])
    elif (topic == "pressure"):
      # in millibars (lowest recorded to highest recorded)
      return str (random.randint (870, 1084))
    elif (topic == "temperature"):
      # in fahrenheit
      return str (random.randint (-100, 100))
    elif (topic == "sound"):
      # in decibels
      return str (random.randint (30, 95))
    elif (topic == "altitude"):
      # in feet
      return str (random.randint (0, 40000))
    elif (topic == "location"):
      return random.choice (["America", "Europe", "Asia", "Africa", "Australia"])