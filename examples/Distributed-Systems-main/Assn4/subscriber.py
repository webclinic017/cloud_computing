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
    
    history_length: int

    def __init__ (self, z_ip: str, z_port: str, topic: str, history_length: int) -> None:
        """ Initialization function

        Arguements:
            broker_ip (str): The ip of the broker

            broker_port (str): The port of the broker

            topic (str): Topic to be interested in

        Returns:
            None
        """

        self.history_length = history_length
        self.get_self_ip()
        self.client = ZKClient(z_ip, z_port)
        self.topic = topic
        self.sockets = []

        self.handle_pub_change(None)
    
    def handle_pub_change(self, event):

        new_pubs = False
        while not new_pubs:
            new_pubs = self.client.get_children(self.topic)
            sleep(1)
            print('waiting for new pubs')

        self.sockets = []
        context = zmq.Context()
        self.poller = zmq.Poller()

        for ip in new_pubs:
            
            port, history_length = self.client.get_value(f'{self.topic}/{ip}').split(":")

            if int(history_length) >= self.history_length:
                socket = context.socket(zmq.SUB)
                socket.connect(f"tcp://{ip}:{port}")
                socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
                self.poller.register (socket, zmq.POLLIN)
                self.sockets.append(socket)
                print(f"Connecting to because of pub change: {ip}: {port}")
            
            else:
                print(f"Not connecting to because history is too small: {ip}: {port}")

        

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
    
    def __init__ (self, z_ip: str, z_port: str, topic: str, history_length: int):
        """ Initialization function

        Arguements:
            broker_ip (str): The ip of the broker

            broker_port (str): The port of the broker

            topic (str): Topic to be interested in

        Returns:
            None
        """

        self.history_length = history_length

        self.get_self_ip()
        self.client = ZKClient(z_ip, z_port)
        self.topic = topic
        self.register()
    
    def register(self, event = None):

        print("in register")
        broker_ip, broker_port = self.get_broker_info()
        self.bp = BrokerProxy(broker_ip, broker_port)
        
        brokers = self.bp.register("SUB", self.topic, f"tcp://{self.ip}: None", self.history_length)
        self.sockets = []
        context = zmq.Context()
        self.poller = zmq.Poller()
            
        for broker in brokers:
            print(broker)
            broker_ip, broker_port = broker.split(":")
            self.bp = BrokerProxy(broker_ip, broker_port)

            pub_port = self.bp.get_pub_port()
            
            socket = context.socket(zmq.SUB)
            socket.connect(f"tcp://{broker_ip}:{pub_port}")
            socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
            self.poller.register(socket, zmq.POLLIN)
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

            print("Waiting to recieve")
            events = dict(self.poller.poll(1000))
            for s in self.sockets:
                
                if s in events:
                    message = s.recv_string()
                    history_length = message.split(" ")[-1]

                    if int(history_length) >= self.history_length:
                        return message
                    else:
                        print(f"Not recieving because history is too small: {history_length}")
            
            self.client.client.get('/BROKER', watch=self.register)
            self.client.client.get('/BALANCER', watch=self.register)
            sleep(1)
