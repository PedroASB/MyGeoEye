import socket
import threading
import os
from serialization import *

PORT = 5052
HOST_NAME = socket.gethostname()
SERVER_IP = socket.gethostbyname(HOST_NAME)
ADDRESS = (SERVER_IP, PORT)
IMAGES_DIR = 'images_database'

def save_image(connection, directory):
    """Armazena a imagem"""
    image_name = deserialize_string(connection)
    image_size = deserialize_int(connection)
    image_path = os.path.join(directory, image_name)
    with open(image_path, 'wb') as file:
        received_size = 0
        while received_size < image_size:
            data = connection.recv(CHUNK_SIZE)
            file.write(data)
            received_size += len(data)
    print(f'Imagem "{image_name}" armazenada com sucesso.')


def list_images(connection, directory):
    """Lista todas as imagens que o cliente salvou"""
    images = os.listdir(directory)
    num_images = len(images)
    serialize_int(connection, num_images)
    if num_images > 0:
        serialize_string(connection, '\n'.join(images))


def handle_client(connection, address):
    """Gerencia a conexão com um cliente"""
    print(f'[NOVA CONEXÃO] Cliente {address[0]}:{address[1]} conectado.')
    while True:
        option = deserialize_int(connection)
        match option:
            case 1: # Inserir imagem
                print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                save_image(connection, IMAGES_DIR)
                
            case 2: # Baixar imagem
                print(f'[COMANDO] {address[0]}:{address[1]}: Baixar imagem.')
                print('<Imagem enviada para o cliente>')

            case 3: # Listar imagens
                print(f'[COMANDO] {address[0]}:{address[1]}: Listar imagens.')
                list_images(connection, IMAGES_DIR)
                    
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
    """Inicializa o servidor"""
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
