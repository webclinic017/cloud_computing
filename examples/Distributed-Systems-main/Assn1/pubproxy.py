###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the publisher proxy in the middleware layer
#
# Created: Spring 2022
#
###############################################

# A proxy for the publisher will be used in a remote procedure call
# approach.  We envision its use on the broker side when
# it delegates the work to the proxy to talk to its real counterpart. 
# One may completely avoid this approach if pure message passing is
# going to be used and not have a higher level remote procedure call approach.
import publisher
from topiclist import TopicList

from time import sleep
import time
import sys
import random

class PublisherProxy:
    
    type: str
    topics: list
    pub: publisher.Publisher
    
    def __init__(self, type: str, broker_ip: str, broker_port: str, topics: list, pub_port: str = "") -> None:
        """ Initialization function

        Arguements:
            type (str): Type of dessimination strategy

            broker_ip (str): IP of the broker

            broker_port (str): Port of the broker

            topics (list): List of applicable topcis

            pub_port (str): Port to publish on

        Returns:
            None
        """
        
        self.type = type
        self.topics = topics

        if self.type == "Direct":
            self.pub = publisher.DirectPublisher(broker_ip, broker_port, pub_port, topics)
        
        elif self.type == "Dissemination":
            self.pub = publisher.ViaBrokerPublisher(broker_ip, broker_port, topics)   # Send random list of topics
            
    
    def publish(self, topic: str, value: str) -> None:
        """ Calls publish function on publisher object

        Arguements:
            topic (str): The topic to publish on

            value (str): Value to publish on

        Returns:
            None
        """
        

        print(f"Publishing~{topic} {value}~{time.time()}")
        self.pub.publish(topic, value)

    def publish_rand_topic(self, value: str) -> None:
        """ Publish a random topic by calling publisher function

        Arguements:
            topic (str): The topic to publish on

            value (str): Value to publish on

        Returns:
            None
        """
        
        topic = random.choice(self.topics)
        print(f"Publishing~{topic} {value}~{time.time()}")

        self.pub.publish(topic, value)

def broker_test() -> None:
    """ Creates new publisher proxy and begins to publish
    random topics

    Arguements:
        None

    Returns:
        None
    """

    broker_ip = sys.argv[2]
    broker_port = sys.argv[3]

    topic_list = TopicList()
    topics = topic_list.interest()

    if 'pressure' not in topics:
        topics.append('pressure')
        topics.append('humidity')

    p = PublisherProxy("Dissemination", broker_ip, broker_port, topics)

    while(True):

        p.publish_rand_topic(p.pub.ip)
        sleep(2)

def direct_test() -> None:
    """ Creates new publisher proxy and begins to publish
    random topics

    Arguements:
        None

    Returns:
        None
    """

    broker_ip = sys.argv[2]
    broker_port = sys.argv[3]
    pub_port = sys.argv[4]

    topic_list = TopicList()
    topics = topic_list.interest()

    if 'pressure' not in topics:
        topics.append('pressure')


    p = PublisherProxy("Direct", broker_ip, broker_port, topics, pub_port)

    while(True):

        p.publish_rand_topic(p.pub.ip)
        sleep(2)


def main():

    if sys.argv[1] == "broker":
        broker_test()

    elif sys.argv[1] == "direct":
        direct_test()
    

    

if __name__ == '__main__':
    main()

# Must all to register with list of topics