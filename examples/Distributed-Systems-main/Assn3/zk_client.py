from kazoo.client import KazooClient

class ZKClient:

    host_ip: str
    host_port: str

    client: KazooClient

    def __init__(self, host_ip: str, host_port: str) -> None:
        
        self.host_ip = host_ip
        self.host_port = host_port

        self.client = KazooClient(hosts=f"{host_ip}:{host_port}")         # Can we pass in multiple hosts and does that automatically handle the ensamble server?
        self.client.start()
    
    def create_znode(self, key: str, value: str, ephemeral: bool = True):
        
        if self.client.exists(f"/{key}"):
            self.client.delete(f"/{key}")
            
        print(f"Creating node of key: {key} - value: {value}")
        self.client.create(f"/{key}", value=str.encode(value), ephemeral=ephemeral, makepath=True)
    
    def create_broker_znode(self, key: str, value: str):
        
        print(f"Creating node of key: {key} - value: {value}")
        self.client.create(f"/BROKER/{key}_", value=str.encode(value), ephemeral=True, sequence=True, makepath=True)
            
    def get_children(self, child):
        
        result = self.client.get_children(child)
        return result

    def set_value(self, key: str, value: str):
        
        try:
            self.client.set(f"/{key}", str.encode(value))

        except:
            return

    def get_value(self, znode: str):
        
        try:
            value, stat = self.client.get(f"/{znode}")              # Need to do locking call
            return value.decode("utf-8")

        except:
            return False
