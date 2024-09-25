import socket
import threading
from serialization import *

PORT = 5050
HOST_NAME = socket.gethostname()
SERVER_IP = socket.gethostbyname(HOST_NAME)
ADDRESS = (SERVER_IP, PORT)

def handle_client(connection, address):
    print(f'[NOVA CONEXÃO] Cliente {address[0]}:{address[1]} conectado.')
    while True:
        option = deserialize_int(connection)
        match option:
            case 1: # Inserir imagem
                print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                print('<Imagem inserida>')
                
            case 2: # Baixar imagem
                print(f'[COMANDO] {address[0]}:{address[1]}: Baixar imagem.')
                print('<Imagem enviada para o cliente>')

            case 3: # Listar imagens
                print(f'[COMANDO] {address[0]}:{address[1]}: Listar imagens.')
                print('<Imagens listadas para o cliente>')
                    
            case 4: # Deletar imagem
                print(f'[COMANDO] {address[0]}:{address[1]}: Deletar imagem.')
                print('<Imagem deletada>')

            case 0: # Finalizar
                print(f'[COMANDO] {address[0]}:{address[1]}: Finalizar.')
                break
            
            case _:
                pass
    connection.close()
    print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


def start_server():
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_socket.bind(ADDRESS)
	server_socket.listen()
	print(f'[STATUS] Servidor iniciado em {SERVER_IP}:{PORT}.')
	while True:
		connection, address = server_socket.accept()
		thread = threading.Thread(target=handle_client, args=(connection, address))
		thread.start()
		print(f'Conexões ativas: {threading.active_count() - 1}')


if __name__ == '__main__':
    start_server()