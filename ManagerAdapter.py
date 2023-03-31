###############################################
#
# Author: Sean Steuber
# Vanderbilt University
#
# Purpose: The ManagerAdapter class is a data structure 
# designed to facilitate communication between the 
# Discovery Application and a ZooKeeper instance. 
# It provides methods to track subscribers and publishers 
# by creating and managing znodes in ZooKeeper. 
# This class is intended to be used as an adapter between 
# the Discovery Application and ZooKeeper, simplifying the 
# process of managing znodes and handling connection errors.
#
# Created: Spring 2023
#
###############################################

import json

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.security import OPEN_ACL_UNSAFE

class ManagerAdapter:
    """
    Class for connecting to a ZooKeeper instance and managing
    the data structure used by the Discovery Application to 
    track subscribers and publishers.
    """

    def __init__(self, host, logger):
        """
        Constructor that takes a ZooKeeper host string and connects
        to that instance using the ZooKeeper client library.
        :param host: The host string in the format of hostname:port.
        """
        self.host = host
        self.logger = logger
        
        self.zk = KazooClient(hosts=host)
        self.zk.start()

        self.base_path = "/discovery"
        self.topics_path = self.base_path + "/topics"
        self.dleader_path = self.base_path + "/dleader"
        self.bleader_path = self.base_path + "/bleader"
        
        # Create the necessary nodes in ZooKeeper
        self.zk.ensure_path(self.base_path)
        self.zk.ensure_path(self.topics_path)
        self.zk.ensure_path(self.dleader_path)
        self.zk.ensure_path(self.bleader_path)

    def register_subscriber(self, topic, subscriber):
        """
        Registers a subscriber for a specific topic by adding the
        subscriber's information to the appropriate node in the ZooKeeper
        data structure.
        :param topic: The topic the subscriber is interested in.
        :param subscriber: A dictionary containing the subscriber's 
                           information, such as IP address and port number.
        """
        # Create the necessary nodes in ZooKeeper
        topic_path = self.topics_path + "/" + topic
        subscribers_path = topic_path + "/subscribers"
        self.zk.ensure_path(topic_path)
        self.zk.ensure_path(subscribers_path)

        # Add the subscriber's information to the appropriate node
        subscriber_path = subscribers_path + "/" + subscriber['ip'] + ":" + str(subscriber['port'])
        try:
            self.zk.create(subscriber_path, ephemeral=True, acl=OPEN_ACL_UNSAFE)
            self.logger.debug(f"ManagerAdapter:register_subscriber Successfully registered subscriber for topic {topic}: {subscriber_path}")
        except NodeExistsError:
            self.logger.debug(f"ManagerAdapter:register_subscriber Subscriber {subscriber_path} already registered for topic {topic}")
        except Exception as e:
            self.logger.error(f"ManagerAdapter:register_subscriber Failed to register subscriber for topic {topic}: {e}")

    def unregister_subscriber(self, topic, subscriber):
        """
        Unregisters a subscriber for a specific topic by removing the
        subscriber's information from the appropriate node in the ZooKeeper
        data structure.
        :param topic: The topic the subscriber is no longer interested in.
        :param subscriber: A dictionary containing the subscriber's 
                           information, such as IP address and port number.
        """
        subscriber_path = f'{self.base_path}/{topic}/subscribers/{subscriber["ip"]}:{subscriber["port"]}'
        try:
            if self.zk.exists(subscriber_path):
                self.zk.delete(subscriber_path)
                self.logger.debug(f'ManagerAdapter:unregister_subscriber Successfully unregistered subscriber {subscriber["ip"]}:{subscriber["port"]} from topic {topic}.')
            else:
                self.logger.debug(f'ManagerAdapter:unregister_subscriber Subscriber {subscriber["ip"]}:{subscriber["port"]} is not registered to topic {topic}.')
        except NoNodeError:
            self.logger.debug(f'ManagerAdapter:unregister_subscriber Topic {topic} does not exist in the ZooKeeper data structure.')

    def unregister_subscriber_from_topics(self, subscriber, topics):
        """
        Unregisters a subscriber from a list of topics by removing the
        subscriber's information from the appropriate nodes in the ZooKeeper
        data structure.
        :param subscriber: A dictionary containing the subscriber's 
                        information, such as IP address and port number.
        :param topics: A list of topics to unregister the subscriber from.
        """
        for topic in topics:
            self.unregister_subscriber(topic, subscriber)

    def register_publisher(self, topic, publisher):
        """
        Registers a publisher for a specific topic by adding the
        publisher's information to the appropriate node in the ZooKeeper
        data structure.
        :param topic: The topic the publisher is publishing to.
        :param publisher: A dictionary containing the publisher's 
                        information, such as IP address and port number.
        """
        node_path = "{}/{}/publishers/{}".format(self.base_path, topic, publisher["id"])
        node_data = "{}:{}".format(publisher["ip"], publisher["port"])
        
        try:
            self.zk.create(node_path, node_data.encode('utf-8'), makepath=True, acl=None, ephemeral=True)
        except NodeExistsError:
            self.zk.set(node_path, node_data.encode('utf-8'))

    def unregister_publisher(self, topic, publisher):
        """
        Unregisters a publisher for a specific topic by removing the
        publisher's information from the appropriate node in the ZooKeeper
        data structure.
        :param topic: The topic the publisher is no longer publishing to.
        :param publisher: A dictionary containing the publisher's 
                        information, such as IP address and port number.
        """
        pub_path = self.base_path + "/" + topic + "/publishers/" + publisher["ip"] + ":" + publisher["port"]
        try:
            self.zk.delete(pub_path)
        except NoNodeError:
            self.logger.debug(f"ManagerAdapter:unregister_publisher No publisher node for {topic} topic and publisher {publisher} exists")
        except Exception as e:
            self.logger.error(f"ManagerAdapter:unregister_publisher Failed to delete publisher node for {topic} topic and publisher {publisher}: {e}")

    def unregister_publisher_from_topics(self, publisher, topics):
        """
        Unregisters a publisher from a list of topics by removing the
        publisher's information from the appropriate nodes in the ZooKeeper
        data structure.
        :param publisher: A dictionary containing the publisher's 
                        information, such as IP address and port number.
        :param topics: A list of topics to unregister the publisher from.
        """
        for topic in topics:
            self.unregister_publisher(topic, publisher)

    def get_subscribers_by_topic(self, topic):
        """
        Returns a list of subscribers for a specific topic.
        :param topic: The topic to retrieve the subscribers for.
        :return: A list of dictionaries containing subscriber information.
        """
        try:
            subscribers = []
            if self.zk.exists(self.base_path + '/' + topic):
                children = self.zk.get_children(self.base_path + '/' + topic)
                for child in children:
                    subscriber = self.zk.get(self.base_path + '/' + topic + '/' + child)[0].decode("utf-8")
                    subscribers.append(json.loads(subscriber))
            return subscribers
        except NoNodeError:
            return None

    def get_publishers_by_topic(self, topic):
        """
        Returns a list of all publishers for a topic.
        :param topic: The topic to retrieve publishers for.
        :return: A list of dictionaries containing publisher information.
        """
        publishers = []
        topic_path = self.base_path + '/' + topic + '/publishers'
        if self.zk.exists(topic_path):
            publisher_nodes = self.zk.get_children(topic_path)
            for node in publisher_nodes:
                publisher_path = topic_path + '/' + node
                try:
                    publisher_data, _ = self.zk.get(publisher_path)
                    publishers.append(json.loads(publisher_data))
                except NoNodeError:
                    pass
        return publishers

    def close(self):
        """
        Closes the connection to the ZooKeeper instance.
        """
        self.zk.close()