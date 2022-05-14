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
from numpy import sort
import psutil
import zmq
from dataclasses import dataclass
from time import sleep
import psutil
from kazoo.protocol.states import WatchedEvent
import time
from collections import defaultdict

from messages import DissMessage
from messages import RegistrationMessage
from zk_client import ZKClient

@dataclass 
class Node:
    
    topics: list
    ip: str
    port: str
    history_length: int


class Broker (ABC):

    context: object
    socket: object
    ip: str 
    port: str
    
    @abstractmethod
    def start (self):
        pass

class Broker(Broker):

    prev_time: time.time
    num_requests: int

    pubs: defaultdict = defaultdict(list)
    subs: list = []

    replicas: dict = {}
    
    pub_socket: object
    pub_port: str
    type: str

    client: ZKClient

    leader: bool = False
    
    def __init__ (self, server_port: str, pub_port: str, z_ip: str, z_port: str):
        """ Initialization function for the broker class

        Arguements:
            server_port (str): Port for the server to use

            pub_port (str): Port for the publisher to use

            type (str): Type of dissimination method to be used

        Returns:
            None
        """
        
        print("In Broker")

        self.prev_time = time.time()
        self.num_requests = 0
        self.replicas = {}

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
        self.replicas[self.get_name()] = 0
        
        self.start()

    def get_leader(self):

        children = self.client.client.get_children('/BROKER')

        nodes = {i[0]: i[1] for i in (i.split("_") for i in children)}
        sorted_children = dict(sorted(nodes.items(), key=lambda item: item[1]))
        current_leader = list(sorted_children.keys())[0]

        return current_leader
    
    def get_position(self):

        children = self.client.client.get_children('/BROKER')
        nodes = {i[0]: i[1] for i in (i.split("_") for i in children)}
        sorted_children = dict(sorted(nodes.items(), key=lambda item: item[1]))
        self_pos = list(sorted_children.keys()).index(self.ip)
        
        return self_pos, sorted_children

    def balance_increased_load(self, sorted_children: dict):

        current_balancers = self.client.get_value('/BALANCER').split('-')
        self.num_requests = 0

        if len(current_balancers) < len(sorted_children):
            
            next_broker =  list(sorted_children.keys())[len(current_balancers)]
            next_broker_name = next_broker + "_" + sorted_children[next_broker]
            self.client.set_value('/BALANCER', self.client.get_value('/BALANCER') + "-" + next_broker_name)
            print(f"Expanding: {next_broker_name}")

            for broker in self.client.get_value('/BALANCER').split('-'):
                self.replicas[broker] = 0
            
            self.pubs = defaultdict(list)
            self.subs = []

        else:
            print("Can't expand further")
    
    def balance_decreased_load(self, sorted_children: dict):

        current_balancers = self.client.get_value('/BALANCER').split('-')

        if len(current_balancers) > 1:        
            
            self.client.set_value('/BALANCER', '-'.join(current_balancers[:-1]))
            print(f"Shrinking the cluster: {self.replicas[current_balancers[-1]]}")

            for broker in self.client.get_value('/BALANCER').split('-'):
                self.replicas[broker] = 0
            
            self.pubs = defaultdict(list)
            self.subs = []

        else:
            print("Cant shrink further")

    def handle_load(self):

        self.num_requests += 1
        time_diff = time.time() - self.prev_time

        if self.leader:

            sorted_children = self.get_position()[1]
            
            if self.num_requests > 10:
                self.balance_increased_load(sorted_children)
            
            elif self.num_requests <= 5 and time_diff > 15:
                self.balance_decreased_load(sorted_children)
        
        if  time_diff > 10:
            self.prev_time = time.time()
            self.num_requests = 0   
                
    def trigger_reregistration(self):

        self.client.set_value('/BROKER', f'{self.ip}:{self.port}')

    def get_name(self):

        brokers = self.client.get_children('/BROKER')
        for broker in brokers:
           
            if self.ip in broker:
                return broker

    def check_topic_owndership(self, topic: str, ip: str):

        children = self.client.get_children(f'/{topic}')
        if ip == children[0]:
            return True
        
    def register_znode(self, z_ip: str, z_port: str):

        self.client = ZKClient(z_ip, z_port)
            
        self.client.create_broker_znode(f"{self.ip}", f'{self.ip}:{self.port}')
        self.check_if_leader()

    def check_if_leader(self, children: list = []):

        new_leader = self.get_leader()
        
        if self.ip in new_leader:
            
            print('I am leader!')
            self.leader = True
            self.client.set_value('/BROKER', f'{self.ip}:{self.port}')
            
            if not self.client.client.exists('/BALANCER'):
                self.client.create_znode('/BALANCER', self.get_name())
            
            else:  
                self.client.set_value('/BALANCER', self.get_name())

            return True
        
        else:
            print('I am still not leader :(')
            return False

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
                print()
            
            sleep(1)
            self.client.client.get_children('/BROKER', watch=self.check_if_leader)
  
    def disseminate(self, message: DissMessage) -> None:
        """ Given a dissemination message, send it out using the 
        socket all subscribers are registered to. Only used for broker 
        method

        Arguements:
            message (DissMessage): Message from publisher to send

        Returns:
            None
        """
        
        topic, pub_ip, history_length = message.info()

        if self.check_topic_owndership(topic, pub_ip):
            self.pub_socket.send_string(f"{topic} {pub_ip} {history_length}")
        else:
            print("{} is not the owner of {}".format(pub_ip, topic))

    def register(self, message: RegistrationMessage):
        """ Given a registration method, create a new node
        object and add to the list of pubs or subs

        Arguements:
            message (RegistrationMessage): Message to register a node

        Returns:
            None
        """
        
        type, topics, id, history_length  = message.info()
        ip = id.split("://")[1].split(":")[0]
        port = id.split("://")[1].split(":")[1]
        
        n = Node(topics, ip, port, history_length)
                
        if type == "PUB":

            min_replica = list(dict(sorted(self.replicas.items(), key=lambda item: item[1])).keys())[0]
            self.replicas[min_replica] += 1
            self.pubs[min_replica].append(n)
            print(min_replica)
            return [self.client.get_value(f'BROKER/{min_replica}')]

        elif type == "SUB":
            self.subs.append(n)
            return [self.client.get_value(f'/BROKER/{i}') for i in list(self.replicas.keys())]

    def get_pub_port(self):

        return self.pub_port

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

        self.num_requests += 1     
        self.handle_load()

        if type(message) is RegistrationMessage:

            message = self.register(message) 
            return message
        
        elif type(message) is DissMessage:

            self.disseminate(message)
            return "dessiminated"
        
        elif message == ["pub_port"]:
            return self.get_pub_port()
        
        elif message == ["load"]:
            return self.get_load()
        
        else:
            return
