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
from kademlia_dht import Kademlia_DHT
import random
import os

from dht import DHT
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
    
    dht: DHT

    def __init__ (self, port: str, topics: list):
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
        
        self.dht = DHT()
        self.register_topics()
    
    def check_if_registered(self, topic: str) -> bool:

        result = self.dht.get_value(topic)

        if result and self.ip in result:
            return True

        return False

    def register_topics(self):
        
        print(self.topics)

        for topic in self.topics:

            while(not self.check_if_registered(topic)):

                print(f"Not registered for {topic}")

                result = self.dht.get_value(topic)
                
                if not result:
                    self.dht.set_value(topic, f'{self.id}:{self.port}')
                else:
                    self.dht.set_value(topic, f'{result}-{self.id}:{self.port}')
            
                sleep(random.randint(1,5))
            
            print(f"registered for {topic}")


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

    dht: DHT
    
    def __init__ (self, topics: list):
        """ Initialization function

        Arguements:
            broker_ip (str): The IP of the broker server

            broker_port (str): The port of the broker server

            topic: list

        Returns:
            None
        """
        
        self.dht = DHT()

        self.topics = topics
        self.get_self_ip()
        self.id = f"tcp://{self.ip}:None"
        
        context = zmq.Context()
        self.poller = zmq.Poller()
        self.start_sockert = context.socket(zmq.SUB)
        
        self.initialize_broker()

        message = self.bp.register("PUB", self.topics, self.id)
        start_ip, start_port = message
        
        self.start_sockert.connect(f"tcp://{start_ip}:{start_port}")
        self.start_sockert.setsockopt_string(zmq.SUBSCRIBE, "start")
        self.poller.register(self.start_sockert, zmq.POLLIN)
        
        self.start()
    
    def initialize_broker(self):

        result = self.dht.get_value("broker")
        broker_ip, broker_port = result.split(':')
        self.bp = BrokerProxy(broker_ip.strip(), broker_port.strip())

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
        print(topic)
        print(value)
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






    
