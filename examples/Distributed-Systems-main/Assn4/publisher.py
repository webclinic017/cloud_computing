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
from collections import deque

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

    history_length: int
    history: deque
    
    start_sockert: object
    poller: object
    
    topics: str
    pub_socket: object

    client: ZKClient
     
    def __init__ (self, z_ip: str, z_port: str, port: str, topics: list, history_length: int) -> None:
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

        self.history_length = history_length
        self.history = deque(maxlen=self.history_length)

        self.client = ZKClient(z_ip, z_port)

        context = zmq.Context()
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{self.ip}:{self.port}")

        self.register_topics()

    def register_topics(self):

        for topic in self.topics:
            print(f"Registering: {topic}/{self.ip}")
            self.client.create_znode(f'{topic}/{self.ip}', f'{self.port}:{self.history_length}')

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

    def check_topic_owndership(self, topic: str):

        children = self.client.get_children(f'/{topic}')
        if self.ip == children[0]:
            return True

    def publish (self, topic: str, value: str) -> None:
        """ Publishs message on it's pub socket

        Arguements:
            topic (str): Topic to publish on

            value (str): Value to publish

        Returns:
            None
        """
        
        if self.check_topic_owndership(topic):
            self.pub_socket.send_string(f"{topic} {value}")
        else:
            print(f"Topic {topic} not owned by {self.ip}")
   
# A concrete class that disseminates info via the broker
class ViaBrokerPublisher (Publisher):

    ip: str
    id: str
    topics: str

    history_length: int
    history: deque
    
    start_sockert: object
    poller: object
    
    client: ZKClient

    def __init__ (self, z_ip: str, z_port: str, topics: list, history_length: int) -> None:
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

        self.history_length = history_length
        self.history = deque(maxlen=self.history_length)

        self.get_self_ip()
        self.id = f"tcp://{self.ip}:None"
        
        self.client = ZKClient(z_ip, z_port)

        self.register_topics()
        self.handle_broker_change(None)

        context = zmq.Context()
        self.poller = zmq.Poller()
        self.start_sockert = context.socket(zmq.SUB)
    
    def handle_broker_change(self, event):

        response = False
        while not response:
            response = self.client.get_value('BROKER')
            sleep(1)

        broker_ip, broker_port = str(response).split(":")
        self.bp = BrokerProxy(broker_ip, broker_port)
        broker_ip, broker_port = self.bp.register("PUB", self.topics, self.id, self.history_length)[0].split(":")
        self.bp = BrokerProxy(broker_ip, broker_port)
    
    def register_topics(self):

        for topic in self.topics:
            print(f"Registering: {topic}/{self.ip}")
            self.client.create_znode(f'{topic}/{self.ip}', f' :{self.history_length}')

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
        self.client.client.get('/BALANCER', watch=self.handle_broker_change)
        self.bp.dessiminate(topic, value, self.history_length)
        