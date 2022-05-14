###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker proxy at the middleware layer
#
# Created: Spring 2022
#
###############################################

# If you decide to do the RPC approach, you might need a proxy for the
# real broker.
#
# This is the proxy object for the broker which is held by both the
# publisher and subscriber-side middleware to store info about the
# whereabouts of the actual broker. The application level logic does not
# know that it is talking to a proxy object. It will simply invoke methods
# on the proxy, which then get translated under the hood into the appropriate
# serialization logic and sending to the real broker

 
# Each item holds a broker proxy obeject
# They just have the information on the ip and port of the actual broker so you can make calls to them
import zmq
from dataclasses import dataclass
from messages import DissMessage
from messages import RegistrationMessage

class BrokerProxy:
    
    method: str
    ip: str 
    port: str
    
    socket: object
    context: object
        
    def __init__(self, ip: str, port: str) -> None:
        """ Initialization function. Given the ip and port of 
        the actual broker object, connect a socket to the brokers
        server

        Arguements:
            ip (str): Ip of the broker server

            port (str): Port of the broker server

        Returns:
            None
        """
        
        self.ip = ip
        self.port = port

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{self.ip}:{self.port}")
        
    
    def register(self, type: str, topics: list, id: str) -> object:
        """ Given the type, topics and id of either a publisher
        or a subscriber, send a RegistrationMessage to the broker
        via it's server

        Arguements:
            type (str): Type of node

            topics (list): List of interested or publishing topics

            id: (str)" Id of the node

        Returns:
            message (object): Message sent back by the broker server
        """
        
        self.socket.send_pyobj(RegistrationMessage(type, topics, id))
        message = self.socket.recv_pyobj()
        return message
    
    def dessiminate(self, topic: str, value: str) -> None:
        """ Given the topic and value, dessiminate a message
        through the broker by sending a DissMessage to its server

        Arguements:
            topic (str): Topic to be published on

            value (str): Value to be sent

        Returns:
            None
        """
        
        self.socket.send_pyobj(DissMessage(topic, value))
        message = self.socket.recv_pyobj()
    
    def send_start_message(self) -> None:
        """ Send the start message to the broker server

        Arguements:
            None

        Returns:
            None
        """
        
        print("sending start message")
        self.socket.send_pyobj(f"START")
        message = self.socket.recv_pyobj()
    
    def send_connect_message(self):
        """ Send the connect message to the broker server

        Arguements:
            None

        Returns:
            None
        """
        
        self.socket.send_pyobj(f"CONNECT")
        message = self.socket.recv_pyobj()

    

    