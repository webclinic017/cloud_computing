import multiprocessing as mp
import psutil
from time import sleep
import sys

from broker import Broker
from brokerproxy import BrokerProxy


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

    sleep(15)

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
