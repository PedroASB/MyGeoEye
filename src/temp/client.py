import rpyc

# Conectar ao servidor
conn = rpyc.connect("localhost", 18861)

try:
    # Chamando a função remota que gera uma exceção
    conn.root.raise_exception()
except rpyc.core.vinegar.GenericException as e:
    # Capturando a exceção propagada do servidor
    # print(f"Exceção remota capturada: {e}")
    # print("Detalhes da exceção remota:")
    print(e)
except Exception as e:
    # Captura qualquer outra exceção local
    print(f"Erro ocorrido")


print('Hello world')