###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# See the cs6381_publisher.py file for how an abstract Publisher class is
# defined and then two specialized classes. We may need similar things here.
# I am also assuming that discovery and dissemination are lumped into the
# broker. Otherwise keep them in separate files.

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
import psutil
import zmq
from dataclasses import dataclass
from time import sleep
import psutil
from kazoo.protocol.states import WatchedEvent
import ast

from messages import DissMessage
from messages import RegistrationMessage
from zk_client import ZKClient

@dataclass 
class Node:
    
    topics: list
    ip: str
    port: str


class Broker (ABC):

    context: object
    socket: object
    ip: str 
    port: str
    
    @abstractmethod
    def start (self):
        pass

class Broker(Broker):

    pubs: list = []
    subs: list = []
    
    pub_socket: object
    pub_port: str
    type: str

    client: ZKClient

    def __init__ (self, server_port: str, pub_port: str, type: str, z_ip: str, z_port: str):
        """ Initialization function for the broker class

        Arguements:
            server_port (str): Port for the server to use

            pub_port (str): Port for the publisher to use

            type (str): Type of dissimination method to be used

        Returns:
            None
        """
        
        print("In Broker")

        self.get_self_ip()
        self.port = server_port
        self.pub_port = pub_port
        self.type = type

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://{self.ip}:{self.port}")
        
        pub_context = zmq.Context()
        self.pub_socket = pub_context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{self.ip}:{self.pub_port}")

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        
        self.register_znode(z_ip, z_port)
        self.start()

    def get_leader(self, children):
        
        if children:

            application_nodes = [{"node": i[0], "sequence": i[1]} for i in (i.split("_") for i in children)]
            current_leader = min(application_nodes, key=lambda x: x["sequence"])['node']
            return current_leader

        else:
            return -1
    
    
    def register_znode(self, z_ip: str, z_port: str):

        self.client = ZKClient(z_ip, z_port)
            
        self.client.create_broker_znode(f"{self.ip}", f'{self.ip}:{self.port}')
        self.check_if_leader()

    def check_if_leader(self, children: list = []):

        new_leader = self.get_leader(self.client.client.get_children('/BROKER'))
        
        if self.ip in new_leader:
            print('I am leader!')
            self.client.set_value('/BROKER', f'{self.ip}:{self.port}')
        
        else:
            print('I am still not leader :(')
            

    def get_self_ip(self) -> None:
        """ Uses the psutil to get the actual ip address
        of the host it's being run on

        Arguements:
            None

        Returns:
            None
        """

        addrs = psutil.net_if_addrs()
        for key in addrs:
            if "h" in key:
                self.ip = addrs[key][0].address

    def start(self) -> None:
        """ Function to loop and weight for requests to 
        come from the server

        Arguements:
            None

        Returns:
            None
        """
        
        while True:

            print('waiting')
            events = dict(self.poller.poll(1000))
            if self.socket in events:

                message = self.socket.recv_pyobj()
                return_message = self.handle_request(message)
                self.socket.send_pyobj(return_message)
                print(message)
                print(return_message)
            
            sleep(1)
            self.client.client.get_children('/BROKER', watch=self.check_if_leader)
  

    def find_publishers(self, message: RegistrationMessage) -> list:
        """ Given a registration from a subscriber, return the 
        information for all publishers that have the same topic

        Arguements:
            message (RegistrationMessage): Message from subcriber to register

        Returns:
            publishers (list): List of all publishers who use the same topic
        """
        
        type, topics = message.info()[:-1]

        if type == "SUB":
            
            matching_pubs = []
            for pub in self.pubs:
                
                if topics[0] in pub.topics:
                    matching_pubs.append([pub.ip, pub.port])
            
            if matching_pubs:
                return matching_pubs
            else:
                return [[self.ip, self.pub_port]]                     # If sub doesnt have a match then connect it to the broker
        
        else:
            return [[self.ip, self.pub_port]]
                    
           
    def disseminate(self, message: DissMessage) -> None:
        """ Given a dissemination message, send it out using the 
        socket all subscribers are registered to. Only used for broker 
        method

        Arguements:
            message (DissMessage): Message from publisher to send

        Returns:
            None
        """
        
        topic, value = message.info()
        self.pub_socket.send_string(f"{topic} {value}")
    

    def register_topics(self, pub: Node):
        
        for topic in pub.topics:

            print(f"Registering: {topic}/{pub.ip}")
            self.client.create_znode(f'{topic}/{pub.ip}', pub.port)


    def register(self, message: RegistrationMessage):
        """ Given a registration method, create a new node
        object and add to the list of pubs or subs

        Arguements:
            message (RegistrationMessage): Message to register a node

        Returns:
            None
        """
        
        type, topics, id  = message.info()
        ip = id.split("://")[1].split(":")[0]
        port = id.split("://")[1].split(":")[1]
        
        n = Node(topics, ip, port)
                
        if type == "PUB":
            self.pubs.append(n)
            self.register_topics(n)

        elif type == "SUB":
            self.subs.append(n)
    
    def handle_request(self, message: object):
        """ Given a message object, dictate if its a
        registration request or a dissementation method
        and handle appropriatley

        Arguements:
            message (object): Message to handle

        Returns:
            response (object): Either a list of nodes or confirmation
            string
        """
        
    
            
        if type(message) is RegistrationMessage:

            self.register(message)

            if self.type == "dessiminate":
                return [self.ip, self.pub_port]

            elif self.type == "direct":
                return self.find_publishers(message)
        
        elif type(message) is DissMessage:

            self.disseminate(message)
            return "dessiminated"
        
        else:
            return
