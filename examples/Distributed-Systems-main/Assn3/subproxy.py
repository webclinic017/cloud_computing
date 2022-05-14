###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the subscriber proxy in the middleware layer
#
# Created: Spring 2022
#
###############################################

# A proxy for the subscriber will be used in a remote procedure call
# approach.  We envision its use on the broker side when
# it delegates the work to the proxy so the proxy can communicate with
# its real counterpart.  One may completely avoid this approach
# if pure message passing is going to be used and not have a higher level
# remote procedure call approach.

import sys
from time import sleep
import time

import subscriber as subscriber

class SubscriberProxy:
    
    type: str
    sub: subscriber.Subscriber
    
    def __init__(self, type: str, broker_ip: str, broker_port: str, topic: str) -> None:
        """ Initialization function for SubscriberProxy

        Arguements:
            type (str): Type of dissemination method

            broker_ip (str): IP of the broker server

            broker_port (str): Port of the broker server

            topic (str): Topic to be interested in

        Returns:
            None
        """
        
        self.type = type
        
        if self.type == "Direct":
            self.sub = subscriber.DirectSubscriber(broker_ip, broker_port, topic)
        
        elif self.type == "Broker":
            self.sub = subscriber.ViaBrokerSubscriber(broker_ip, broker_port, topic)
            
    
    def recieve(self):
        """ Waits for message from subscriber object

        Arguements:
            None

        Returns:
            None
        """
        
        message = self.sub.recieve()
        print(f"Recieved-{message}-{time.time()}")
        return message
