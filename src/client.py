import socket
import os
from serialization import *

FORMAT = 'utf-8'
HOST_NAME = socket.gethostname()
SERVER_IP = socket.gethostbyname(HOST_NAME)
PORT = 5050
ADDRESS = (SERVER_IP, PORT)
IMAGES_DIR = 'client_images'

def upload_image(client_socket, directory, image_name):
    """Envia uma imagem para o servidor"""
    serialize_string(client_socket, image_name)
    image_path = os.path.join(directory, image_name)
    image_size = os.path.getsize(image_path)
    serialize_int(client_socket, image_size)
    with open(image_path, 'rb') as file:
        while chunk := file.read(CHUNK_SIZE):
            client_socket.send(chunk)
    print(f'\nImagem "{image_name}" enviada com sucesso.')


def download_image(client_socket, directory, image_name):
    """Baixa uma imagem do servidor"""
    serialize_string(client_socket, image_name)
    has_image = deserialize_bool(client_socket)
    if not has_image:
        print('\n[ERRO] Arquivo de imagem não encontrado.')
        return
    image_size = deserialize_int(client_socket)
    image_path = os.path.join(directory, image_name)
    with open(image_path, 'wb') as file:
        received_size = 0
        while received_size < image_size:
            data = client_socket.recv(CHUNK_SIZE)
            file.write(data)
            received_size += len(data)
    print(f'Imagem "{image_name}" armazenada com sucesso em "{directory}/".')


def list_images(client_socket):
    """Lista as imagens que já foram salvas"""
    num_images = deserialize_int(client_socket)
    if num_images > 0:
        images = deserialize_string(client_socket)
        print('\nImagens armazenadas:\n' + images)
    else:
        print('\nNenhuma imagem armazenada.')


def delete_image(client_socket, image_name):
    """Deleta uma imagem"""
    serialize_string(client_socket, image_name)
    has_image = deserialize_bool(client_socket)
    if has_image:
        print(f'\nImagem "{image_name}" deletada com sucesso.')
    else:
        print('\n[ERRO] Arquivo de imagem não encontrado.')


def start_client():
    """Inicializa o cliente"""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect(ADDRESS)
    except Exception:
        print('[ERRO] Não foi possível estabelecer conexão com o servidor.')
        exit(0)
    print('[STATUS] Conexão com o servidor estabelecida.')

    while True:
        print('\nSelecione um comando:')
        print('1 - Enviar imagem')
        print('2 - Baixar imagem')
        print('3 - Listar imagens')
        print('4 - Deletar imagem')
        print('0 - Finalizar')
        command = input('>> ')
        match command:
            case '1': # Inserir imagem
                image_name = str(input('\nImagem a ser enviada: '))
                image_path = os.path.join(IMAGES_DIR, image_name)
                if not os.path.exists(image_path):
                    print('\n[ERRO] Arquivo de imagem não encontrado.')
                    continue
                serialize_int(client_socket, 1)
                upload_image(client_socket, IMAGES_DIR, image_name)

            case '2': # Baixar imagem
                serialize_int(client_socket, 2)
                image_name = str(input('\nImagem a ser baixada: '))
                download_image(client_socket, IMAGES_DIR, image_name)

            case '3': # Listar imagens
                serialize_int(client_socket, 3)
                list_images(client_socket)

            case '4': # Deletar imagem
                serialize_int(client_socket, 4)
                image_name = str(input('\nImagem a ser deletada: '))
                delete_image(client_socket, image_name)

            case '0': # Encerrar conexão
                serialize_int(client_socket, 0)
                break

            case _:
                print('\n[ERRO] Comando inválido.')
    
    client_socket.close()
    print('\n[STATUS] Conexão com o servidor encerrada.')


if __name__ == '__main__':
    start_client()
