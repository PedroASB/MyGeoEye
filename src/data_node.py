import rpyc, os, time, pika, json, psutil, threading
from rpyc.utils.server import ThreadedServer

# Data node
INDEX = 1
PORT_DATA_NODE = 8000 + INDEX
HOST_DATA_NODE = '192.168.40.111' # 'localhost'
NAME_DATA_NODE = f'data_node_{INDEX}'

# Serviço de nomes
HOST_NAME_SERVICE = '192.168.40.223' # 'localhost'
PORT_NAME_SERVICE = 6000

# RabbitMQ
RABBITMQ_HOST_MONITOR_STATUS = '192.168.40.244' # 'localhost'
RABBITMQ_HOST_MONITOR_SCORE = '192.168.40.137' # 'localhost'

# Tamanho de chunks / fragmentos
CHUNK_SIZE = 65_536     # 64 KB
SHARD_SIZE = 2_097_152  # 2 MB

class PubResources:
    QUEUE_DATA_NODE_RESOURCES = 'queue_data_node_resources'  # Nome da fila específica para este data node
    EXCHANGE_DATA_NODE_RESOURCES = 'exchange_data_node_resources'
    VERIFICATION_INTERVAL = 20 # Tempo em segundos entre cada verificação

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST_MONITOR_SCORE))
        self.channel = self.connection.channel()
        # Declara a fila única para este monitor
        self.channel.exchange_declare(exchange=self.EXCHANGE_DATA_NODE_RESOURCES, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_DATA_NODE_RESOURCES)


    def start_monitoring_resouces(self):
        print("[STATUS] Iniciando publisher de recursos do data node.")
        try:
            while True:
                self.notify_subs()
                time.sleep(self.VERIFICATION_INTERVAL)  # Intervalo entre as verificações
        except KeyboardInterrupt:
            print("[STATUS] Encerrando publisher de recursos do data node.")
            self.connection.close()


    def notify_subs(self):
        message_dict = self.get_node_resources()
        message_dict['node_id'] = NAME_DATA_NODE
        message_json = json.dumps(message_dict)
        
        self.channel.basic_publish(exchange='', routing_key=self.QUEUE_DATA_NODE_RESOURCES, body=message_json)
        print(f"[INFO] Notificação enviada para {self.QUEUE_DATA_NODE_RESOURCES}: {message_json}")


    def get_node_resources(self):
        """Retorna o status dos recursos do nó."""
        cpu_usage = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory().percent
        disk_info =  psutil.disk_usage('/').percent
        return {'cpu': cpu_usage, 'memory': memory_info, 'disk': disk_info}


class PubStatus:
    EXCHANGE_DATA_NODE_STATUS = 'exchange_data_node_status'
    QUEUE_DATA_NODE_STATUS = 'queue_data_node_status'  # Nome da fila específica para este data node
    VERIFICATION_INTERVAL = 10 # Tempo em segundos entre cada verificação

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST_MONITOR_STATUS))
        self.channel = self.connection.channel()
        # Declara a fila única para este monitor
        self.channel.exchange_declare(exchange=self.EXCHANGE_DATA_NODE_STATUS, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_DATA_NODE_STATUS)


    def start_monitoring_status(self):
        print("[STATUS] Iniciando publisher de recursos do data node.")
        try:
            while True:
                self.notify_subs()
                time.sleep(self.VERIFICATION_INTERVAL)  # Intervalo entre as verificações
        except KeyboardInterrupt:
            print("[STATUS] Encerrando publisher de recursos do data node.")
            self.connection.close()


    def notify_subs(self):
        message_dict = {}
        message_dict['node_id'] = NAME_DATA_NODE
        message_dict['time'] = time.time()
        message_json = json.dumps(message_dict)
        self.channel.basic_publish(exchange='', routing_key=self.QUEUE_DATA_NODE_STATUS, body=message_json)
        print(f"[INFO] Notificação enviada para {self.QUEUE_DATA_NODE_STATUS}: {message_json}")


class DataNode(rpyc.Service):
    STORAGE_DIR = f'data_node_storage_{INDEX}'

    def __init__(self, host, port):
        if not os.path.exists(self.STORAGE_DIR):
            os.makedirs(self.STORAGE_DIR)
        self.open_files = {}
        self.pub_resources = PubResources()
        self.pub_status = PubStatus()
        self.host = host
        self.port = port
    

    def exposed_clear_storage_dir(self):
        """Remove todos os arquivos e subdiretórios do diretório de armazenamento."""
        if os.path.exists(self.STORAGE_DIR):
            for root, dirs, files in os.walk(self.STORAGE_DIR, topdown=False):
                # Remove todos os arquivos
                for file in files:
                    os.remove(os.path.join(root, file))


    def exposed_store_image_chunk(self, image_name, shard_index, image_chunk): #try_exception
        try:
            # Cria o diretório onde os chunks serão salvos, se necessário
            image_shard_name = f'{image_name}%part{shard_index}%'
            image_path = os.path.join(self.STORAGE_DIR, image_shard_name)
            # Armazena o chunk recebido
            with open(image_path, "ab") as file:
                file.write(image_chunk)
        except Exception as exception:
            print('[ERRO] Erro em exposed_store_image_chunk')
            raise exception


    def exposed_retrieve_image_chunk(self, image_name, shard_index): #try_exception
        """
        Envia chunks de uma parte de imagem para o servidor de forma incremental.
        """
        try:
            image_shard_name = f'{image_name}%part{shard_index}%'
            image_path = os.path.join(self.STORAGE_DIR, image_shard_name)
            
            # Verificar se o arquivo já está aberto ou não
            if image_shard_name not in self.open_files:
                if not os.path.exists(image_path):
                    print(f"[ERRO] {image_name}%part{shard_index}% não encontrada.")
                    return None
                # Abrir o arquivo e armazenar no mapeamento
                self.open_files[image_shard_name] = open(image_path, "rb")

            file = self.open_files[image_shard_name]
            image_chunk = file.read(CHUNK_SIZE)
            eof = False
            
            # Significa que temos o último "chunk" ou vazio
            if len(image_chunk) < CHUNK_SIZE:
                file.close()
                del self.open_files[image_shard_name]
                print(f"[STATUS] Leitura de {image_name}%part{shard_index}% concluída.")
                eof = True
            
            # for of in self.open_files:
            #     print(of)
            
            return image_chunk, eof
        except Exception as exception:
            print('[ERRO] Erro em exposed_retrieve_image_chunk')
            try:
                file.close()
            except Exception:
                pass
            if image_shard_name in self.open_files:
                del self.open_files[image_shard_name]
            raise exception


    def exposed_store_image_shard(self, image_shard_name, image_shard): #try_exception
        try:
            image_shard_path = os.path.join(self.STORAGE_DIR, image_shard_name)
            with open(image_shard_path, "wb") as file:
                file.write(image_shard)
        except Exception as exception:
            print('[ERRO] Erro em exposed_store_image_shard')
            raise exception


    def exposed_retrieve_image_shard(self, image_shard_name): #try_exception
        try:
            image_shard_path = os.path.join(self.STORAGE_DIR, image_shard_name)
            with open(image_shard_path, "rb") as file:
                image_shard = file.read(SHARD_SIZE)
            return image_shard
        except Exception as exception:
            print('[ERRO] Erro em exposed_retrieve_image_shard')
            raise exception


    def exposed_delete_image(self, image_name, shard_index): #try_exception
        try:
            image_shard_name = f'{image_name}%part{shard_index}%'
            image_path = os.path.join(self.STORAGE_DIR, image_shard_name)
            if os.path.exists(image_path):
                os.remove(image_path)
                print(f'[STATUS] Fragmento de imagem "{image_shard_name}" deletado com sucesso.')
            else:
                print(f'[STATUS] Fragmento de imagem "{image_shard_name}" não encontrado para deletar.')
        except Exception as exception:
            print('[ERRO] Erro em exposed_delete_image')
            raise exception

    def start(self):
        main_thread = threading.Thread(target=self.start_data_node, daemon=True)
        pub_score_thread = threading.Thread(target=self.pub_resources.start_monitoring_resouces, daemon=True)
        pub_status_thread = threading.Thread(target=self.pub_status.start_monitoring_status, daemon=True)
        
        # Inicia as threads
        pub_score_thread.start()
        pub_status_thread.start()
        main_thread.start()

        # Aguarda as threads finalizarem
        # pub_score_thread.join()
        # pub_status_thread.join()
        # main_thread.join()

        try:
            while main_thread.is_alive() or \
                pub_score_thread.is_alive() or \
                pub_status_thread.is_alive():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("[KEYBOARD_INTERRUPT] Encerrando as threads.")


    def register_name(self, name, host_name_service, port_name_service):
        try:
            name_service_conn = rpyc.connect(host_name_service, port_name_service)
        except ConnectionRefusedError:
            print('[ERRO] Não foi possível estabelecer conexão com o serviço de nomes.')
            return False
        if not name_service_conn.root.lookup(name):
            name_service_conn.root.register(name, self.host, self.port)
        print(f'[STATUS] Servidor registrado no serviço de nomes como "{name}".')
        return True


    def start_data_node(self):
        threaded_data_node = ThreadedServer(service=self,
                                            port=PORT_DATA_NODE,
                                            protocol_config={'allow_public_attrs': True})
        print(f'[STATUS] Data node iniciado na porta {PORT_DATA_NODE}.')
        threaded_data_node.start()
        

if __name__ == "__main__":
    data_node = DataNode(HOST_DATA_NODE, PORT_DATA_NODE)
    if data_node.register_name(NAME_DATA_NODE, HOST_NAME_SERVICE, PORT_NAME_SERVICE):
        data_node.start()
