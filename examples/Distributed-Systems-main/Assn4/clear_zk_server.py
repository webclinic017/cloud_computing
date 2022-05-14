import os
from kazoo.client import KazooClient

os.system('sudo zookeeper/bin/zkServer.sh stop; sudo zookeeper/bin/zkServer.sh start')

client = KazooClient(hosts=f"10.0.0.1:2181")         
client.start()

children = client.get_children(f'/')

print(children)
for kid in children:
    
    if kid != "zookeeper":
        client._delete_recursive(f'/{kid}')