import socket
import threading
import time
import sys

# --- SIMULATED KAFKA BROKER ---
def simulated_kafka_broker(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server_socket.bind((host, port))
        server_socket.listen(1)
        print(f"[Broker] Kafka broker (simulated) listening on {host}:{port}")
        conn, addr = server_socket.accept()
        with conn:
            print(f"[Broker] Connected by {addr}")
            conn.sendall(b"Hello from simulated Kafka broker!")
            data = conn.recv(1024)
            if data:
                print(f"[Broker] Received: {data.decode()}")
    except OSError as e:
        print(f"[Broker] Error starting broker on {host}:{port}: {e}")
    finally:
        server_socket.close()

# --- SIMULATED KAFKA CLIENT ---
def simulated_kafka_client(broker_address, client_id):
    host, port = broker_address
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"[{client_id}] Attempting to connect to {host}:{port}...")
    try:
        client_socket.connect((host, port))
        print(f"[{client_id}] Successfully connected to {host}:{port}")
        data = client_socket.recv(1024)
        print(f"[{client_id}] Received from broker: {data.decode()}")
        client_socket.sendall(f"Message from {client_id}".encode())
    except ConnectionRefusedError:
        print(f"[{client_id}] Connection refused to {host}:{port}. (Broker not listening or network issue)")
    except socket.timeout:
        print(f"[{client_id}] Connection timed out to {host}:{port}. (Network unreachable)")
    except OSError as e:
        print(f"[{client_id}] Network error connecting to {host}:{port}: {e}")
    finally:
        client_socket.close()
    print(f"[{client_id}] Disconnected.")

def main():
    # --- Kafka Addressing & VPC Peering Concepts ---

    # In a real Kafka setup, brokers have 'listeners' and 'advertised.listeners'.
    # 'listeners' define the interfaces Kafka binds to.
    # 'advertised.listeners' tell clients how to connect to the broker.

    # We simulate this with different IP addresses and connection attempts.

    # Scenario 1: Broker in VPC A, Client in VPC A (or VPC B via Peering)
    # The broker listens on an "internal" IP (e.g., 10.0.0.1 in VPC A).
    # VPC Peering allows clients in VPC B to reach this internal IP directly,
    # without traversing the public internet, ensuring secure and efficient communication.
    internal_broker_ip = "127.0.0.1" # Represents an internal IP in VPC A
    broker_port = 9092

    # Start the simulated broker in a separate thread
    broker_thread = threading.Thread(target=simulated_kafka_broker, args=(internal_broker_ip, broker_port))
    broker_thread.daemon = True # Allow main thread to exit even if broker is running
    broker_thread.start()
    time.sleep(1) # Give broker time to start

    print("\n--- Scenario 1: Client connects to internal IP (VPC A or VPC B via Peering) ---")
    # Client in VPC A connects to internal IP (should succeed)
    simulated_kafka_client((internal_broker_ip, broker_port), "Client-VPC-A")
    time.sleep(1)

    # Client in VPC B attempts to connect to the same internal IP.
    # If VPC Peering is configured, this connection would succeed as if it were local.
    # Without peering, this connection would fail unless the IP is publicly routable.
    # In this simulation, it will succeed because 127.0.0.1 is local.
    # The key takeaway is that VPC Peering makes an internal IP *reachable* across VPCs.
    simulated_kafka_client((internal_broker_ip, broker_port), "Client-VPC-B-via-Peering")
    time.sleep(1)

    print("\n--- Scenario 2: Client attempts to connect to an unreachable internal IP ---")
    # This simulates a client in VPC B trying to reach an internal IP in VPC A
    # *without* VPC Peering, or an IP that simply doesn't exist/isn't routable.
    # This connection attempt should fail.
    unreachable_broker_ip = "127.0.0.2" # Represents an internal IP in a different, unpeered VPC
    simulated_kafka_client((unreachable_broker_ip, broker_port), "Client-VPC-B-No-Peering")
    time.sleep(1)

    print("\n--- Scenario 3: Client attempts to connect to a 'public' IP (conceptual) ---")
    # In a real Kafka setup, a broker might have a public listener (e.g., 0.0.0.0:9093).
    # Clients would connect to the public DNS/IP. This is less secure and higher latency
    # compared to VPC Peering for inter-VPC communication.
    # We won't start another broker for this, just illustrate the attempt.
    public_broker_ip = "8.8.8.8" # Represents a public IP (Google DNS for demonstration of unreachability)
    public_broker_port = 9093
    print(f"[Client-Public] Attempting to connect to conceptual public IP {public_broker_ip}:{public_broker_port}...")
    try:
        # Set a short timeout for public connections to fail quickly
        public_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        public_client_socket.settimeout(2)
        public_client_socket.connect((public_broker_ip, public_broker_port))
        print(f"[Client-Public] Successfully connected to {public_broker_ip}:{public_broker_port} (unexpected for 8.8.8.8:9093)")
        public_client_socket.close()
    except socket.timeout:
        print(f"[Client-Public] Connection timed out to {public_broker_ip}:{public_broker_port}. (Expected for a non-existent service on a public IP)")
    except ConnectionRefusedError:
        print(f"[Client-Public] Connection refused to {public_broker_ip}:{public_broker_port}. (Expected for a non-existent service)")
    except OSError as e:
        print(f"[Client-Public] Network error connecting to {public_broker_ip}:{public_broker_port}: {e}")
    print("[Client-Public] Disconnected.")

    print("\nSimulation finished. The broker thread will terminate when the main program exits.")
    # In a real scenario, the broker would run indefinitely.
    # For this example, we let the daemon thread die with the main program.

if __name__ == "__main__":
    main()
