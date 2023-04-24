###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher middleware code
#
# Created: Spring 2023
#
###############################################

# This file contains any declarations that are common to all middleware entities

# For now we do not have anything here but you can add enumerated constants for
# the role we are playing and any other common things that we need across
# all our middleware objects. Make sure then to import this file in those files once
# some content is added here that is needed by others. 

import datetime
import json

##################################
#       Parcel class
#  used as a common data structure
# for published messages
##################################
class TopicParcel:

    ########################################
    # constructor
    ########################################
    def __init__ (self, topic, data, publisher, ownership = 0, sent_at = datetime.datetime.now().isoformat(sep=" ")):
        self.topic = topic           # Registrant name (some unique name)
        self.data = data             # topic data
        self.publisher = publisher # name of the publisher
        self.ownership = ownership # ownership strength for pub to topic
        self.sent_at =  sent_at   # current timestamp

    @classmethod
    def fromMessage(self, parcelString):
        msgJson = parcelString.split(":",1)[1] # get just the json and not the topic as well
        parcelDict = json.loads(msgJson) #
        topic = parcelDict["topic"]           # Registrant name (some unique name)
        data = parcelDict["data"]             # topic data
        publisher = parcelDict["publisher"] # name of the publisher
        ownership = parcelDict["ownership"] # ownership strength for pub to topic
        sent_at = parcelDict["sent_at"]    # current timestamp
        return TopicParcel(topic, data, publisher, ownership, sent_at)
        
    def __str__(self):
     return self.topic + ":" + json.dumps(
        {
            "topic":self.topic, 
            "data":self.data, 
            "publisher":self.publisher,
            "ownership":self.ownership,
            "sent_at":self.sent_at
        })
