import threading
import time

# Função simples que será executada pela thread
def worker(name):
    print(f"Thread {name} iniciada.")
    time.sleep(4)  # Simula algum trabalho
    print(f"Thread {name} concluída.")

def start():
    # Criação de threads
    thread1 = threading.Thread(target=worker, args=("A",), daemon=True)
    thread2 = threading.Thread(target=worker, args=("B",), daemon=True)

    # Inicia as threads
    thread1.start()
    thread2.start()

    # Monitora as threads para ver se ainda estão vivas
    try:
        while thread1.is_alive() or thread2.is_alive():
            # Pode adicionar um tempo de espera para evitar o uso excessivo de CPU
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nInterrupção recebida. Encerrando programa.")

    print("Fim do programa.")

if __name__ == "__main__":
    start()
