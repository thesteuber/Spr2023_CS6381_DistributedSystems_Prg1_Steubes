###############################################
#
# Author: Sean Steuber
# Vanderbilt University
#
# Purpose: Data Structure for the Discovery Application 
# to track subscribers and publishers.
#
# Created: Spring 2023
#
###############################################

##################################
#       Registrant class
##################################
class Registrant:
    ########################################
    # constructor
    ########################################
    def __init__ (self, name, address, port, topic_list):
        self.name = name             # Registrant name (some unique name)
        self.address = address       # Registrant Address (IP)
        self.port = port             # Registrant Port
        self.topic_list = topic_list # Registrant Topic List

    ########################################
    # constructor
    ########################################
    def __init__ (self, name):
        self.name = name          # Registrant name (some unique name)
        self.address = None       # Registrant Address (IP)
        self.port = None          # Registrant Port
        self.topic_list = None    # Registrant Topic List

##################################
#       DiscoveryLedger class
##################################
class DiscoveryLedger ():

    ########################################
    # constructor
    ########################################
    def __init__ (self):
        self.subscribers = list() # List of subscriber registrants
        self.publishers = list()  # List of publisher registrants