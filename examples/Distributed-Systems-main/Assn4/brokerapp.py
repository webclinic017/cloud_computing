import multiprocessing as mp
import psutil
from time import sleep
import sys

from broker import Broker
from brokerproxy import BrokerProxy


def start_broker(server_port: str, pub_port: str, z_ip: str, z_port: str,):
    """ Given the server and pub port, start a new broker instance

    Arguements:
        server_port (str): Port of the server

        pub_port (str): Port of the publisher socket

        type (str): Type of dissemenation strategy

    Returns:
        None
    """

    b = Broker(server_port, pub_port, z_ip, z_port)

def broker_test():
    """ Parses the server and pub port from command arguemnts,
    creates a new broker on a separate process, waits 5 seconds
    and sends the start message via the broker proxy

    Arguements:
        None

    Returns:
        None
    """
    
    sleep(5)
    server_port = sys.argv[2]
    pub_port = sys.argv[3]
    z_ip = sys.argv[4]
    z_port = sys.argv[5]

    b = mp.Process(target=start_broker, args=(server_port, pub_port, z_ip, z_port))
    b.start()


def main():
    
    broker_test()

   
   

if __name__ == '__main__':
    main()
