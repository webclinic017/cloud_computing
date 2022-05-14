###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the middleware layer for the publisher functionality
#
# Created: Spring 2022
#
###############################################

# ABC stands for abstract base class and this is how Python library
# defines the underlying abstract base class
from abc import ABC, abstractmethod
import zmq
import psutil
from time import sleep
from zk_client import ZKClient
from uuid import uuid4

from brokerproxy import BrokerProxy


# define an abstract base class for the publisher
class Publisher (ABC):

    bp: BrokerProxy

    @abstractmethod
    def publish (self, topic, value):
        pass

class DirectPublisher (Publisher):

    ip: str
    port: str
    id: str
    
    start_sockert: object
    poller: object
    
    topics: str
    pub_socket: object

    client: ZKClient
    
    def __init__ (self, z_ip: str, z_port: str, port: str, topics: list):
        """ Initialization function

        Arguements:
            broker_ip (str): Ip of the broker server

            broker_port (str): Port of the broker server

            port (str): Port to publish on

            topics (list): Topics to publish on

        Returns:
            None
        """

        self.get_self_ip()
        self.port = port
        self.topics = topics
        self.id = f"tcp://{self.ip}:{self.port}"

        self.client = ZKClient(z_ip, z_port)

        broker_ip, broker_port = self.get_broker_info()
        self.bp = BrokerProxy(broker_ip, broker_port)
        message = self.bp.register("PUB", self.topics, self.id)             # No longer waiting for start as it doesn't work in this context
        print(f'Registration message: {message}')
        context = zmq.Context()
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{self.ip}:{self.port}")


    def get_broker_info(self):
        
        response = False

        while not response:

            response = self.client.get_value('BROKER')
            print('Waiting for response for broker')
            sleep(1)
        
        print(f"Got brokers information: {response}")
        broker_ip, broker_port = str(response).split(":")
        return broker_ip, broker_port


    def get_self_ip(self) -> None:
        """ Gets actual IP of host

        Arguements:
            None

        Returns:
            None
        """

        addrs = psutil.net_if_addrs()
        for key in addrs:
            if "h" in key:
                self.ip = addrs[key][0].address

    def handle_broker_change(self, event):

        response = self.client.get_value('BROKER')
        print(f"Got new brokers information: {response}")
        broker_ip, broker_port = str(response).split(":")
        self.bp = BrokerProxy(broker_ip, broker_port)
        print('Started Re-registering on pubs side')
        message = self.bp.register("PUB", self.topics, self.id)
        print('Finished Re-registering on pubs side')

    def publish (self, topic: str, value: str) -> None:
        """ Publishs message on it's pub socket

        Arguements:
            topic (str): Topic to publish on

            value (str): Value to publish

        Returns:
            None
        """
        
        self.client.client.get('/BROKER', watch=self.handle_broker_change)
        self.pub_socket.send_string(f"{topic} {value}")
   
# A concrete class that disseminates info via the broker
class ViaBrokerPublisher (Publisher):

    ip: str
    id: str
    topics: str
    
    start_sockert: object
    poller: object
    
    client: ZKClient

    def __init__ (self, z_ip: str, z_port: str, topics: list):
        """ Initialization function

        Arguements:
            broker_ip (str): The IP of the broker server

            broker_port (str): The port of the broker server

            topic: list

        Returns:
            None
        """
        
        self.topics = list(set(topics))
        print(self.topics)

        self.get_self_ip()
        self.id = f"tcp://{self.ip}:None"
        
        self.client = ZKClient(z_ip, z_port)

        broker_ip, broker_port = self.get_broker_info()
        self.bp = BrokerProxy(broker_ip, broker_port)
        message = self.bp.register("PUB", self.topics, self.id)     # No longer using start method from broker because it doesn't make sense when pubs can leave and join

        context = zmq.Context()
        self.poller = zmq.Poller()
        self.start_sockert = context.socket(zmq.SUB)
                
    def get_broker_info(self):
        
        response = False

        while not response:

            response = self.client.get_value('BROKER')
            print('Waiting for response for broker')
            sleep(1)
        
        print("Got brokers information")
        broker_ip, broker_port = str(response).split(":")
        print(broker_ip)
        print(broker_port)
        return broker_ip, broker_port
    
    def handle_broker_change(self, event):

        response = self.client.get_value('BROKER')
        print(f"Got new brokers information: {response}")
        broker_ip, broker_port = str(response).split(":")
        self.bp = BrokerProxy(broker_ip, broker_port)
        print('Started Re-registering on pubs side')
        message = self.bp.register("PUB", self.topics, self.id)
        print('Finished Re-registering on pubs side')
            

    def get_self_ip(self) -> None:
        """ Gets actual IP from host

        Arguements:
            None

        Returns:
            None
        """

        addrs = psutil.net_if_addrs()
        for key in addrs:
            if "h" in key:
                self.ip = addrs[key][0].address

    def publish (self, topic: str, value: str) -> None:
        """ Publishs value by calling dessimatate on bp, 
        which sends message on brokers server

        Arguements:
            topic (str): Topic to publish on

            value (str): Value to publish

        Returns:
            None
        """

        self.client.client.get('/BROKER', watch=self.handle_broker_change)
        self.bp.dessiminate(topic, value)
        