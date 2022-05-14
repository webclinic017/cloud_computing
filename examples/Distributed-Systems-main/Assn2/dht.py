from kademlia.network import Server  # this is a higher level class
import asyncio     # the Kademlia library uses the asynchronous I/O
import multiprocessing as mp

class DHT:

    ip: str
    port: int
    server: Server

    def __init__(self, ip: str) -> None:

        self.ip = ip
        

    def get_value(self, key):
        
        shared_list = mp.Manager().list()
        p = mp.Process(target=get_value, args=(self.ip, key, shared_list))
        p.start()
        p.join()

        return shared_list[0]
    

    def set_value(self, key, value):
        
       p = mp.Process(target=set_value, args=(self.ip, key, value))
       p.start()
       p.join()
    

def set_value(ip, key, value):

    server = Server()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(server.listen(8468))
    bootstrap_node = (ip, 8468)
    loop.run_until_complete(server.bootstrap([bootstrap_node]))
    loop.run_until_complete(server.set(key, value))
    
    server.stop()
    loop.close()
    print("done setting value")

def get_value(ip, key, shared_list: list):
        
        server = Server()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(server.listen(8468))
        bootstrap_node = (ip, 8468)
        loop.run_until_complete(server.bootstrap([bootstrap_node]))

        result = loop.run_until_complete(server.get(key))
        
        server.stop()
        loop.close()
        print("done getting value")
        shared_list.append(result)
