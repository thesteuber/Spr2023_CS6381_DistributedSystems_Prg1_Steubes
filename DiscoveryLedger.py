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
        self.broker = None # Singular broker in case a ViaBroker is leveraged

    def remove_registrant(self, name):
        """
        Removes any publishers or subscribers with the given name from the ledger
        """
        # Remove publishers
        self.publishers = [p for p in self.publishers if p.name != name]
        
        # Remove subscribers
        self.subscribers = [s for s in self.subscribers if s.name != name]

        if self.broker.name == name:
            self.broker = None