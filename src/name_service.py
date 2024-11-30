import rpyc
from rpyc.utils.server import ThreadedServer

HOST_NAME_SERVICE = 'localhost'
PORT_NAME_SERVICE = 6000

class NameService(rpyc.Service):
    def __init__(self):
        self.registry = {}

    def exposed_register(self, name, host, port):
        self.registry[name] = (host, port)
        print(f'[NEW REGISTRY] Servidor "{name}" registrado em {host}:{port}.')

    def exposed_lookup(self, name):
        return self.registry.get(name, False)

    def exposed_list_servers(self):
        return self.registry
    
    def start(self, port):
        t = ThreadedServer(service=self, port=port)
        print(f'[STATUS] Serviço de nomes iniciado na porta {port}')
        t.start()


if __name__ == "__main__":
    name_service = NameService()
    name_service.start(PORT_NAME_SERVICE)