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
from dataclasses import dataclass
from brokerproxy import BrokerProxy
import zmq
import psutil
from time import sleep

# define an abstract base class for the publisher
class Publisher (ABC):

    bp: BrokerProxy

    @abstractmethod
    def publish (self, topic, value):
        pass

    @abstractmethod
    def start (self):
        pass


class DirectPublisher (Publisher):

    ip: str
    port: str
    id: str
    
    start_sockert: object
    poller: object
    
    topics: str
    pub_socket: object
    
    def __init__ (self, broker_ip: str, broker_port: str, port: str, topics: list):
        """ Initialization function

        Arguements:
            broker_ip (str): Ip of the broker server

            broker_port (str): Port of the broker server

            port (str): Port to publish on

            topics (list): Topics to publish on

        Returns:
            None
        """
        
        self.port = port
        self.topics = topics
        self.get_self_ip()
        self.id = f"tcp://{self.ip}:{self.port}"
        
        context = zmq.Context()
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{self.ip}:{self.port}")
        
        context = zmq.Context()
        self.poller = zmq.Poller()
        self.start_sockert = context.socket(zmq.SUB)
        
        self.bp = BrokerProxy(broker_ip, broker_port)
        message = self.bp.register("PUB", self.topics, self.id)
        ip, port = message[0]
        
        self.start_sockert.connect(f"tcp://{ip}:{port}")
        self.start_sockert.setsockopt_string(zmq.SUBSCRIBE, "start")
        self.poller.register(self.start_sockert, zmq.POLLIN)
        
        self.start()
    
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


    def publish (self, topic: str, value: str) -> None:
        """ Publishs message on it's pub socket

        Arguements:
            topic (str): Topic to publish on

            value (str): Value to publish

        Returns:
            None
        """
        
        self.pub_socket.send_string(f"{topic} {value}")
        
    def start (self) -> None:
        """ Waits for start message from the broker server

        Arguements:
            None

        Returns:
            None
        """
        
        while True:

            print('waiting for start')
            
            events = dict(self.poller.poll())
            
            if self.start_sockert in events:
                message = self.start_sockert.recv_string()
                return
            sleep(1)

# A concrete class that disseminates info via the broker
class ViaBrokerPublisher (Publisher):

    ip: str
    id: str
    topics: str
    
    start_sockert: object
    poller: object
    
    def __init__ (self, broker_ip: str, broker_port: str, topics: list):
        """ Initialization function

        Arguements:
            broker_ip (str): The IP of the broker server

            broker_port (str): The port of the broker server

            topic: list

        Returns:
            None
        """
        
        self.topics = topics
        self.get_self_ip()
        self.id = f"tcp://{self.ip}:None"
        
        context = zmq.Context()
        self.poller = zmq.Poller()
        self.start_sockert = context.socket(zmq.SUB)
        
        self.bp = BrokerProxy(broker_ip, broker_port)
        message = self.bp.register("PUB", self.topics, self.id)
        start_ip, start_port = message
        
        self.start_sockert.connect(f"tcp://{start_ip}:{start_port}")
        self.start_sockert.setsockopt_string(zmq.SUBSCRIBE, "start")
        self.poller.register(self.start_sockert, zmq.POLLIN)
        
        self.start()
    
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
        
        self.bp.dessiminate(topic, value)

    def start (self) -> None:
        """ Waits for start message from broker server

        Arguements:
            None

        Returns:
            None
        """
        
        while True:
            
            events = dict(self.poller.poll())
            
            if self.start_sockert in events:
                print("Recieved start")
                return






    
