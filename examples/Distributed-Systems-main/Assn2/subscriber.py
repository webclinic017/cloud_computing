###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the subscriber functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# Please see the corresponding hints in the cs6381_publisher.py file
# to see how an abstract class is defined and then two specialized classes
# are defined based on the dissemination approach. Something similar
# may have to be done here. If dissemination is direct, then each subscriber
# will have to connect to each separate publisher with whom we match.
# For the ViaBroker approach, the broker is our only publisher for everything.

from abc import ABC, abstractmethod
import zmq
import psutil
from kademlia_dht import Kademlia_DHT
import random
from dht import DHT
import os

from brokerproxy import BrokerProxy
# define an abstract base class for the publisher

class Subscriber (ABC):

    bp: BrokerProxy
    
    @abstractmethod
    def recieve(self):
        pass

# a concrete class that disseminates info directly
class DirectSubscriber (Subscriber):

    ip: str
    port: str
    topic: str
    
    context: object
    sockets: list
    poller: object
    start_socket: object

    dht = DHT
    
    def __init__ (self, topic: str):
        """ Initialization function

        Arguements:
            broker_ip (str): The ip of the broker

            broker_port (str): The port of the broker

            topic (str): Topic to be interested in

        Returns:
            None
        """
        
        print("IN subscriber")
        self.dht = DHT()

        self.topic = topic
        self.get_self_ip()

        self.connect_to_pubs()
            

    def connect_to_pubs(self):

        context = zmq.Context()
        self.poller = zmq.Poller()
        self.sockets = []

        while (True):
            
            result = self.dht.get_value(self.topic)
            
            print(result)
            if result:

                for pub in result.split('-'):

                    pub_ip, pub_port = pub.split(':')
                    print(f"Connecting to: {pub_ip}: {pub_port}")

                    socket = context.socket(zmq.SUB)
                    socket.connect(f"tcp://{pub_ip}:{pub_port}")
                    socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
                    self.poller.register (socket, zmq.POLLIN)
                    self.sockets.append(socket)

                break

    def get_self_ip(self):
        """ Gets IP of actual host

        Arguements:
            None

        Returns:
            None
        """

        addrs = psutil.net_if_addrs()
        for key in addrs:
            if "h" in key:
                self.ip = addrs[key][0].address

    
    def recieve (self):
        """ Polls for messages from each publisher its subscribed to


        Arguements:
            None

        Returns:
            None
        """

        while True:
            
            events = dict(self.poller.poll())
            for s in self.sockets:
                
                if s in events:
                    message = s.recv_string()
                    return message



class ViaBrokerSubscriber (Subscriber):

    ip: str
    topic: str
    
    context: object
    socket: object

    dht: DHT
    
    def __init__ (self, topic: str):
        """ Initialization function

        Arguements:
            broker_ip (str): The ip of the broker

            broker_port (str): The port of the broker

            topic (str): Topic to be interested in

        Returns:
            None
        """
        
        self.dht = DHT()

        self.topic = topic
        self.ip = self.get_self_ip()
        
        self.initialize_broker()

        message = self.bp.register("SUB", self.topic, f"tcp://{self.ip}: None")  
        self.sub_ip, self.sub_port = message
        
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(f"tcp://{self.sub_ip}:{self.sub_port}")
        self.socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
    
    def initialize_broker(self):
        
        result = self.dht.get_value("broker")
        broker_ip, broker_port = result.split(':')
        self.bp = BrokerProxy(broker_ip.strip(), broker_port.strip())

    def get_self_ip(self):
        """ Gets IP of actual host

        Arguements:
            None

        Returns:
            None
        """

        addrs = psutil.net_if_addrs()
        for key in addrs:
            if "h" in key:
                self.ip = addrs[key][0].address


    def recieve (self):
        """ Continually weights from message from server

        Arguements:
            None

        Returns:
            None
        """
        
        message = self.socket.recv_string()
        return message






    
