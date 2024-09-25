import socket
import os
from serialization import *

FORMAT = 'utf-8'
HOST_NAME = socket.gethostname()
SERVER_IP = socket.gethostbyname(HOST_NAME)
PORT = 5050
ADDRESS = (SERVER_IP, PORT)

def start_client():
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
                serialize_int(client_socket, 1)
                print('\n<Imagem inserida>')

            case '2': # Baixar imagem
                serialize_int(client_socket, 2)
                print('\n<Imagens baixadas>')

            case '3': # Listar imagens
                serialize_int(client_socket, 3)
                print('\n<Imagens listadas>')

            case '4': # Deletar imagem
                serialize_int(client_socket, 3)
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