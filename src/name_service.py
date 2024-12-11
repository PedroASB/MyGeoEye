import rpyc, re
from itertools import islice
from rpyc.utils.server import ThreadedServer

HOST_NAME_SERVICE = 'localhost'
PORT_NAME_SERVICE = 6000

class NameService(rpyc.Service):
    def __init__(self):
        self.registry = {}

    def exposed_register(self, name, host, port):
        self.registry[name] = (host, port)
        print(f'[NEW REGISTRY] Serviço "{name}" registrado em {host}:{port}.')

    def exposed_lookup(self, name):
        return self.registry.get(name, False)

    def exposed_lookup_data_nodes(self, quantity=None):
        pattern = re.compile(r"^data_node_\d+$")
        data_nodes = {key: value for key, value in self.registry.items() if pattern.match(key)}
        if quantity is not None:
            data_nodes = dict(islice(data_nodes.items(), quantity))
        return data_nodes
    
    def exposed_list_servers(self):
        return self.registry
    
    def start(self, port):
        t = ThreadedServer(service=self, port=port, protocol_config={'allow_public_attrs': True})
        print(f'[STATUS] Serviço de nomes iniciado na porta {port}')
        t.start()


if __name__ == "__main__":
    name_service = NameService()
    name_service.start(PORT_NAME_SERVICE)