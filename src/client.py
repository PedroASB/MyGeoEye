import socket
import os
from serialization import *

FORMAT = 'utf-8'
HOST_NAME = socket.gethostname()
SERVER_IP = socket.gethostbyname(HOST_NAME)
PORT = 5052
ADDRESS = (SERVER_IP, PORT)

def send_image(client_socket, image_name):
    """Envia uma imagem para o servidor"""
    if not os.path.exists(image_name):
        print('\n[ERRO] Arquivo de imagem não encontrado.')
        return
    serialize_string(client_socket, image_name)
    image_size = os.path.getsize(image_name)
    serialize_int(client_socket, image_size)
    with open(image_name, 'rb') as file:
        while chunk := file.read(CHUNK_SIZE):
            client_socket.send(chunk)
    print('Imagem enviada com sucesso.')


def list_images(client_socket):
    """Lista as imagens que já foram salvas"""
    num_images = deserialize_int(client_socket)
    if num_images > 0:
        images = deserialize_string(client_socket)
        print('\nImagens armazenadas:\n' + images)
    else:
        print('\nNenhuma imagem armazenada.')


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
        # TODO: Enviar/baixar/deletar vários de uma vez
        print('1 - Enviar imagem')
        print('2 - Baixar imagem')
        print('3 - Listar imagens')
        print('4 - Deletar imagem')
        print('0 - Finalizar')
        command = input('>> ')
        match command:
            case '1': # Inserir imagem
                image_name = str(input('\nImagem a ser enviada: '))
                serialize_int(client_socket, 1)
                send_image(client_socket, image_name)

            case '2': # Baixar imagem
                serialize_int(client_socket, 2)
                print('\n<Imagens baixadas>')

            case '3': # Listar imagens
                serialize_int(client_socket, 3)
                list_images(client_socket)

            case '4': # Deletar imagem
                serialize_int(client_socket, 4)
                print('\n<Imagem deletada>')

            case '0':
                serialize_int(client_socket, 0)
                break

            case _:
                print('\n[ERRO] Comando inválido.')
    
    client_socket.close()
    print('\n[STATUS] Conexão com o servidor encerrada.')


if __name__ == '__main__':
    start_client()
