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
from zk_client import ZKClient
from time import sleep
from uuid import uuid4

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

    client: ZKClient
    
    def __init__ (self, z_ip: str, z_port: str, topic: str):
        """ Initialization function

        Arguements:
            broker_ip (str): The ip of the broker

            broker_port (str): The port of the broker

            topic (str): Topic to be interested in

        Returns:
            None
        """

        self.get_self_ip()

        self.client = ZKClient(z_ip, z_port)

        broker_ip, broker_port = self.get_broker_info()
        self.bp = BrokerProxy(broker_ip, broker_port)
        self.topic = topic
        self.sockets = []
        
        message = self.bp.register("SUB", [self.topic], f"tcp://{self.ip}: None")        # Maybe get polled and thats when I can register my node
        print(message)

        context = zmq.Context()
        self.poller = zmq.Poller()

        for pub in message:                                                                 # Need to check this, or have event handler actually, pubs can leave and join

            ip, port = pub
            print(f"Connecting to: {ip}: {port}")

            socket = context.socket(zmq.SUB)
            socket.connect(f"tcp://{ip}:{port}")
            socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
            self.poller.register (socket, zmq.POLLIN)
            self.sockets.append(socket)

    def get_broker_info(self):
        
        response = False

        while not response:

            response = self.client.get_value('BROKER')
            print('Waiting for response for broker')
            sleep(1)
        
        print("Got brokers information")
        broker_ip, broker_port = str(response).split(":")
        return broker_ip, broker_port

    
    def handle_pub_change(self, event):

        print("Handling a publisher change")
        
        new_pubs = self.client.get_children(f'{self.topic}')
        self.sockets = []
        context = zmq.Context()
        self.poller = zmq.Poller()

        for ip in new_pubs:
            
            port = self.client.get_value(f'{self.topic}/{ip}')
            socket = context.socket(zmq.SUB)
            socket.connect(f"tcp://{ip}:{port}")
            socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
            self.poller.register (socket, zmq.POLLIN)
            self.sockets.append(socket)
            print(f"Connecting to because of pub change: {ip}: {port}")

        

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

            events = dict(self.poller.poll(1000))
            for s in self.sockets:
                
                if s in events:
                    message = s.recv_string()
                    return message
            
            self.client.client.get_children(self.topic, watch=self.handle_pub_change)



class ViaBrokerSubscriber (Subscriber):

    ip: str
    topic: str
    
    context: object
    socket: object

    client: ZKClient
    poller = zmq.Poller
    
    def __init__ (self, z_ip: str, z_port: str, topic: str):
        """ Initialization function

        Arguements:
            broker_ip (str): The ip of the broker

            broker_port (str): The port of the broker

            topic (str): Topic to be interested in

        Returns:
            None
        """
        
        self.get_self_ip()

        self.client = ZKClient(z_ip, z_port)

        broker_ip, broker_port = self.get_broker_info()
        self.bp = BrokerProxy(broker_ip, broker_port)
        self.topic = topic

        self.register()
    
    def register(self):

        message = self.bp.register("SUB", self.topic, f"tcp://{self.ip}: None")  
        ip, port = message
        
        self.sub_ip = ip
        self.sub_port = port
        
        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(f"tcp://{self.sub_ip}:{self.sub_port}")
        self.socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
        self.poller.register (self.socket, zmq.POLLIN)

    def get_broker_info(self):
        
        response = False

        while not response:

            response = self.client.get_value('BROKER')
            print('Waiting for response for broker')
            sleep(1)
        
        print("Got brokers information")
        broker_ip, broker_port = str(response).split(":")
        return broker_ip, broker_port

    def handle_broker_change(self, event):

        response = self.client.get_value('BROKER')
        print(f"Got new brokers information: {response}")
        broker_ip, broker_port = str(response).split(":")
        self.bp = BrokerProxy(broker_ip, broker_port)
        self.register()

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
        
        while True:

            events = dict(self.poller.poll(1000))   
            if self.socket in events:

                print("before broker change")
                self.client.client.get('/BROKER', watch=self.handle_broker_change)
                print("after broker change")
                message = self.socket.recv_string()
                return message


