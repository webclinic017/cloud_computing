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
from xxlimited import Str
import psutil
import zmq
from dataclasses import dataclass
from time import sleep
import multiprocessing as mp
import sys
import psutil

from brokerproxy import BrokerProxy
from messages import DissMessage
from messages import RegistrationMessage
from kademlia_dht import Ka

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

# a concrete class that disseminates info directly
class Broker(Broker):

    pubs: list = []
    subs: list = []
    
    pub_socket: object
    pub_port: str
    type: str

    def __init__ (self, server_port: str, pub_port: str, type: str):
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
        
        self.start()

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
            message = self.socket.recv_pyobj()
            return_message = self.handle_request(message)
            self.socket.send_pyobj(return_message)
            print(message)
            print(return_message)
    
    def allow_publishers(self) -> None:
        """ Sends start signal to the publishers
        to allow them to begin sending messages

        Arguements:
            None

        Returns:
            None
        """
        
        self.pub_socket.send_string("start")
    
    
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

        elif message == "START":

            self.allow_publishers()
            return "started"

        
        else:
            return


def start_broker(server_port: str, pub_port: str, type: str):
    """ Given the server and pub port, start a new broker instance

    Arguements:
        server_port (str): Port of the server

        pub_port (str): Port of the publisher socket

        type (str): Type of dissemenation strategy

    Returns:
        None
    """

    b = Broker(server_port, pub_port, type)

def broker_test():
    """ Parses the server and pub port from command arguemnts,
    creates a new broker on a separate process, waits 5 seconds
    and sends the start message via the broker proxy

    Arguements:
        None

    Returns:
        None
    """

    server_port = sys.argv[2]
    pub_port = sys.argv[3]

    b = mp.Process(target=start_broker, args=(server_port, pub_port, "dessiminate", ))
    b.start()

    sleep(5)

    broker_ip = ""
    addrs = psutil.net_if_addrs()
    for key in addrs:
        if "h" in key:
            broker_ip = addrs[key][0].address
            
    bp = BrokerProxy(broker_ip, server_port)

    bp.send_start_message()

def direct_test():
    """ Parses the server and pub port from command arguemnts,
    creates a new broker on a separate process, waits 10 seconds
    and sends the start message via the broker proxy

    Arguements:
        None

    Returns:
        None
    """

    server_port = sys.argv[2]
    pub_port = sys.argv[3]

    b = mp.Process(target=start_broker, args=(server_port, pub_port, "direct", ))
    b.start()

    sleep(10)

    broker_ip = ""
    addrs = psutil.net_if_addrs()
    for key in addrs:
        if "h" in key:
            broker_ip = addrs[key][0].address
            
    bp = BrokerProxy(broker_ip, server_port)

    bp.send_start_message()




def main():
    
    print(sys.argv[1])
    if sys.argv[1] == "broker":
        broker_test()

    elif sys.argv[1] == "direct":
        direct_test()
   

if __name__ == '__main__':
    main()


    

