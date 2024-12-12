import rpyc

# Definindo o serviço remoto
class MyService(rpyc.Service):
    def exposed_raise_exception(self):
        # Função remota que gera uma exceção
        x = 5 / 0
        y = x + 2
        return y

# Iniciando o servidor
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(MyService, port=18861)
    print("Servidor iniciado na porta 18861.")
    server.start()
