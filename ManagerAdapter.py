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
import os

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.security import OPEN_ACL_UNSAFE

from DiscoveryLedger import DiscoveryLedger, Registrant

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
        self.bleader_callback_handle = None
        self.dleader_callback_handle = None
        
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

        self.bleader_node = None
        
        # Create an ephemeral sequential node for broker leader election
        self.bleader_node = self.zk.create(
            self.bleader_path + '/broker-', 
            sequence=True, 
            ephemeral=True, 
            makepath=True, 
            acl=OPEN_ACL_UNSAFE
        )

        # Callback function for the ephemeral sequential node
        def bleader_callback(data, stat, event):
            if event and event.type == "DELETED":
                self.bleader_node = self.elect_bleader()

                if (self.bleader_callback_handle != None):
                    bleader_data = self.zk.get(self.bleader_path + '/' + self.bleader_node)[0].decode('utf-8')
                    name, ip, port = bleader_data.split(':')
                    self.bleader_callback_handle(ip, port)

        # Register the callback function for the ephemeral sequential node
        self.zk.get(self.bleader_node, watch=bleader_callback)

        self.dleader_node = None

        # Create an ephemeral sequential node for discovery leader election
        self.dleader_node = self.zk.create(
            self.dleader_path + '/discovery-', 
            sequence=True, 
            ephemeral=True, 
            makepath=True, 
            acl=OPEN_ACL_UNSAFE
        )
        
        # Callback function for the ephemeral sequential node
        def dleader_callback(data, stat, event):
            if event and event.type == "DELETED":
                self.dleader_node = self.elect_dleader()

                if (self.dleader_callback_handle != None):
                    dleader_data = self.zk.get(self.dleader_path + '/' + self.dleader_node)[0].decode('utf-8')
                    name, ip, port = dleader_data.split(':')
                    self.dleader_callback_handle(ip, port)
        
        # Register the callback function for the ephemeral sequential node
        self.zk.get(self.dleader_node, watch=dleader_callback)

    def get_dleader_as_registrant(self):
        """
        Retrieves the primary discovery node's information as a Registrant object.
        :return: A Registrant object for the primary discovery node.
        """
        dleader_data = self.zk.get(self.elect_dleader())[0].decode('utf-8')
        self.logger.debug("gManagerAdapter:et_dleader_as_registrant - {}".format(dleader_data))
        name, address, port = dleader_data.split(':')
        return Registrant(name, address, port, None)

    def set_dleader_callback_handle(self, handle_function):
        self.dleader_callback_handle = handle_function

    def set_bleader_callback_handle(self, handle_function):
        self.bleader_callback_handle = handle_function

    def elect_dleader(self):
        """
        Elects the primary discovery by selecting the one with the lowest
        sequence number among all the discovery in the dleader_path.
        :return: The path of the primary discovery's node.
        """
        dleader_nodes = self.zk.get_children(self.dleader_path)
        dleader_nodes = sorted(dleader_nodes)
        return os.path.join(self.dleader_path, dleader_nodes[0])
    
    def get_primary_discovery(self):
        """
        Returns the path of the primary discovery's node.
        :return: The path of the primary discovery's node.
        """
        return self.elect_dleader()

    def register_discovery(self, discovery):
        """
        Registers a discovery node by creating an ephemeral node in ZooKeeper.
        If the current dleader is not set yet, sets it to the newly registered
        discovery node.
        :param discovery: A dictionary containing the discovery's information,
                           such as IP address and port number.
        """
        node_path = f"{self.base_path}/discoveries/{discovery.name}"
        node_data = f"{discovery.name}:{discovery.address}:{discovery.port}"
        
        try:
            self.zk.create(node_path, node_data.encode('utf-8'), makepath=True, acl=None, ephemeral=True)
        except NodeExistsError:
            self.zk.set(node_path, node_data.encode('utf-8'))

        # set dleader to the newly registered discovery node if it is not set yet
        if self.get_dleader() is None:
            self.set_dleader(node_path)

    def get_primary_broker_registrant(self):
        broker_date = self.zk.get(self.elect_bleader())[0].decode('utf-8')
        name, ip, port = broker_date.split(':')
        return Registrant(name, ip, port, [])

    def elect_bleader(self):
        """
        Elects the primary broker by selecting the one with the lowest
        sequence number among all the brokers in the dleader_path.
        :return: The path of the primary broker's node.
        """
        bleader_nodes = self.zk.get_children(self.bleader_path)
        bleader_nodes = sorted(bleader_nodes)
        return os.path.join(self.bleader_path, bleader_nodes[0])
    
    def get_primary_broker(self):
        """
        Returns the path of the primary broker's node.
        :return: The path of the primary broker's node.
        """
        return self.elect_bleader()

    def register_broker(self, broker):
        """
        Registers a broker by creating an ephemeral node in ZooKeeper.
        :param broker: A dictionary containing the broker's information,
                       such as IP address and port number.
        """
        node_path = f"{self.base_path}/brokers/{broker.name}"
        node_data = f"{broker.name}:{broker.address}:{broker.port}"
        
        try:
            self.zk.create(node_path, node_data.encode('utf-8'), makepath=True, acl=None, ephemeral=True)
        except NodeExistsError:
            self.zk.set(node_path, node_data.encode('utf-8'))

    def unregister_broker(self, broker):
        """
        Unregisters a broker by deleting the corresponding node in ZooKeeper.
        :param broker: A dictionary containing the broker's information,
                       such as IP address and port number.
        """
        node_path = f"{self.base_path}/brokers/{broker['name']}"
        
        try:
            self.zk.delete(node_path)
        except NoNodeError:
            pass

    def register_subscriber(self, subscriber):
        """
        Registers a subscriber for a specific topic by adding the
        subscriber's information to the appropriate node in the ZooKeeper
        data structure.
        :param subscriber: A dictionary containing the subscriber's 
                            information, such as IP address and port number.
        """
        for topic in subscriber.topic_list:
            node_path = "{}/{}/subscribers/{}".format(self.topics_path, topic, subscriber.name)
            node_data = "{}:{}:{}".format(subscriber.name, subscriber.address, subscriber.port)

            try:
                self.zk.create(node_path, node_data.encode('utf-8'), makepath=True, acl=None, ephemeral=True)
            except NodeExistsError:
                self.zk.set(node_path, node_data.encode('utf-8'))

    def unregister_subscriber(self, subscriber):
        """
        Unregisters a subscriber by removing the subscriber's information
        from the appropriate node in the ZooKeeper data structure.
        :param subscriber: A dictionary containing the subscriber's information,
                        such as IP address and port number.
        """
        for topic in subscriber.topic_list:
            node_path = "{}/{}/subscribers/{}".format(self.topics_path, topic, subscriber.name)
            try:
                self.zk.delete(node_path)
            except NoNodeError:
                pass

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

    def register_publisher(self, publisher):
        """
        Registers a publisher for a specific topic by adding the
        publisher's information to the appropriate node in the ZooKeeper
        data structure.
        :param topic: The topic the publisher is publishing to.
        :param publisher: A dictionary containing the publisher's 
                        information, such as IP address and port number.
        """
        for topic in publisher.topic_list:
            node_path = "{}/{}/publishers/{}".format(self.topics_path, topic, publisher.name)
            node_data = "{}:{}:{}".format(publisher.name, publisher.address, publisher.port)
            
            try:
                self.zk.create(node_path, node_data.encode('utf-8'), makepath=True, acl=None, ephemeral=True)
            except NodeExistsError:
                self.zk.set(node_path, node_data.encode('utf-8'))

    def unregister_publisher(self, publisher):
        """
        Unregisters a publisher for all topics by removing the
        publisher's information from the appropriate nodes in the ZooKeeper
        data structure.
        :param publisher: A dictionary containing the publisher's 
                        information, such as name, address and port number.
        """
        for topic in publisher.topic_list:
            node_path = "{}/{}/publishers/{}".format(self.topics_path, topic, publisher.name)
            try:
                self.zk.delete(node_path)
            except NoNodeError:
                pass

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

    def get_brokers(self):
        """
        Returns a list of all brokers registered in ZooKeeper.
        :return: A list of Registrant objects representing the brokers.
        """
        brokers = []
        brokers_path = f"{self.base_path}/brokers"
        if self.zk.exists(brokers_path):
            broker_nodes = self.zk.get_children(brokers_path)
            for node in broker_nodes:
                broker_path = f"{brokers_path}/{node}"
                try:
                    broker_data, _ = self.zk.get(broker_path)
                    name, address, port = broker_data.decode("utf-8").split(":")
                    broker = Registrant(name, address, int(port), None)
                    brokers.append(broker)
                except NoNodeError:
                    pass
        return brokers

    def get_publishers(self):
        """
        Returns a list of all publishers registered in the ZooKeeper data structure.
        :return: A list of Registrant objects containing publisher information.
        """
        publishers = []
        try:
            topics = self.zk.get_children(self.topics_path)
            for topic in topics:
                publisher_nodes = self.zk.get_children(self.topics_path + '/' + topic + '/publishers')
                for node in publisher_nodes:
                    publisher_path = self.topics_path + '/' + topic + '/publishers/' + node
                    try:
                        publisher_data, _ = self.zk.get(publisher_path)
                        publisher_name, publisher_address, publisher_port = publisher_data.decode("utf-8").split(':')
                        publisher_topic_list = [topic]
                        publisher = Registrant(publisher_name, publisher_address, publisher_port, publisher_topic_list)

                        # Check if the publisher has already been added to the list
                        existing_publisher = next((p for p in publishers if p.name == publisher_name), None)
                        if existing_publisher is None:
                            publishers.append(publisher)
                        else:
                            # Add the topic to the existing publisher's topic list
                            for p in publishers:
                                if p == publisher:
                                    p.topic_list.append(topic)
                    except NoNodeError:
                        pass
            return publishers
        except NoNodeError:
            return None

    def get_subscribers(self):
        """
        Returns a list of unique subscribers and their topic lists.
        :return: A list of Registrant objects representing subscribers and their topic lists.
        """
        subscribers = []
        try:
            # Iterate over all topics
            for topic in self.zk.get_children(self.topics_path):
                topic_path = self.topics_path + '/' + topic

                # Iterate over all subscribers for this topic
                for subscriber in self.zk.get_children(topic_path + '/subscribers'):
                    subscriber_path = topic_path + '/subscribers/' + subscriber

                    try:
                        # Get the subscriber's data
                        subscriber_data, _ = self.zk.get(subscriber_path)
                        subscriber_data = subscriber_data.decode('utf-8')
                        subscriber_info = subscriber_data.split(':')

                        # Create a Registrant object for this subscriber if it doesn't already exist
                        subscriber_name = subscriber_info[0]
                        subscriber_address = subscriber_info[1]
                        subscriber_port = int(subscriber_info[2])

                        existing_subscriber = next((s for s in subscribers if s.name == subscriber_name), None)
                        if existing_subscriber is None:
                            # Add a new Registrant object to the list of subscribers
                            subscriber_topics = [topic]
                            new_subscriber = Registrant(subscriber_name, subscriber_address, subscriber_port, subscriber_topics)
                            subscribers.append(new_subscriber)
                        else:
                            # Add the topic to the existing subscriber's topic list
                            existing_subscriber.topic_list.append(topic)
                    except NoNodeError:
                        pass
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