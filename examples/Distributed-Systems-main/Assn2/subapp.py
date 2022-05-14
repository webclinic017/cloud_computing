from time import sleep
import sys

from subproxy import SubscriberProxy

def broker_test():
    """ Starts a broker subscriber proxy and 
    continually recieves messages

    Arguements:
        None

    Returns:
        None
    """

    sleep(5)

    broker_ip = sys.argv[2]
    broker_port = sys.argv[3]
    topic = sys.argv[4]

    s = SubscriberProxy("Broker", broker_ip, broker_port, topic)

    while(True):
        message = s.recieve()

def direct_test():
    """ Starts a direct subscriber proxy and 
    continually recieves messages

    Arguements:
        None

    Returns:
        None
    """

    sleep(5)    # Giving time for publisher to connect first

    broker_ip = sys.argv[2]
    broker_port = sys.argv[3]
    topic = sys.argv[4]

    s = SubscriberProxy("Direct", broker_ip, broker_port, topic)

    it = 0
    while(True):

        it += 1
        if it == len(s.sub.sockets) * 3:
            s.sub.connect_to_pubs()
            it = 0
            
        message = s.recieve()


def main():
    
    if sys.argv[1] == "broker":
        broker_test()
    elif sys.argv[1] == "direct":
        direct_test()
    

if __name__ == '__main__':
    main()
