import socket
import threading
import os
import sys
import pickle
import hashlib
import logging
import time
import traceback

logging.basicConfig(filename='chunk_server.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

MASTER_PORT = 7082  # Primary Master Port
HEARTBEAT_INTERVAL = 5

class ChunkServer:
    def __init__(self, host, port, myChunkDir, filesystem):
        self.filesystem = filesystem
        self.myChunkDir = myChunkDir
        self.host = host
        self.port = port
        self.chunkserver_info = []  # List of stored chunks
        self.lease_info = {}  # Lease info for tracking active leases
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        logging.info("Chunk Server initialized on host %s, port %d", host, port)

    def start(self):
        """Start the chunk server, begin listening and send periodic heartbeats."""
        threading.Thread(target=self.send_heartbeat).start()
        self.listen()

    def listen(self):
        """Listen for incoming connections and handle each in a separate thread."""
        self.sock.listen(5)
        logging.info("Chunk Server started, listening on port %d", self.port)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target=self.handle_request, args=(client, address)).start()

    def send_heartbeat(self):
        """Send periodic heartbeat messages to the MasterServer to indicate server activity."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((socket.gethostbyname('localhost'), MASTER_PORT))
                    heartbeat_message = {'command': 'heartbeat', 'port': self.port}
                    s.send(pickle.dumps(heartbeat_message))
                logging.info("Heartbeat sent to MasterServer from port %d", self.port)
            except Exception as e:
                logging.error("Failed to send heartbeat: %s", e)

    def calculate_checksum(self, data):
        """Calculate the checksum of data for integrity checks."""
        return hashlib.sha256(data).hexdigest()

    def connect_to_master(self, filename, chunk_id):
        """Notify the MasterServer of stored chunk and get replication info."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((socket.gethostbyname('localhost'), MASTER_PORT))
                message = {'command': 'chunk_info', 'filename': filename, 'chunk_id': chunk_id, 'port': self.port}
                s.send(pickle.dumps(message))
                response = pickle.loads(s.recv(4096))
                
                if response.get('status') == 'replicate':
                    target_port = response.get('target_port')
                    self.replicate_chunk(filename, chunk_id, target_port)
        except Exception as e:
            logging.error("Failed to communicate with master server: %s", e)

    def replicate_chunk(self, filename, chunk_id, target_port):
        """Replicate the chunk to another chunk server as per MasterServer's instruction."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((socket.gethostbyname('localhost'), target_port))
                path = os.path.join(self.myChunkDir, f"{filename}_{chunk_id}")
                with open(path, 'rb') as f:
                    data = f.read()
                    checksum = self.calculate_checksum(data)
                    s.send(pickle.dumps({'command': 'replicate', 'data': data, 'checksum': checksum, 'chunk_id': chunk_id, 'filename': filename}))
                logging.info("Replicated chunk %s to server on port %d", chunk_id, target_port)
        except Exception as e:
            logging.error("Failed to replicate chunk %s: %s", chunk_id, e)

    def check_lease(self, filename):
        """Check with the Master Server if the file is currently leased."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((socket.gethostbyname('localhost'), MASTER_PORT))
                lease_message = {'command': 'check_lease', 'filename': filename}
                s.send(pickle.dumps(lease_message))
                response = pickle.loads(s.recv(4096))
                return response.get('leased', False)  # Return True if leased, else False
        except Exception as e:
            logging.error("Failed to check lease status with master: %s", e)
            return False  # Assume not leased on failure to contact master

    def store_chunk(self, client, chunk_id, filename, data, checksum):
        """Store chunk data from client, ensuring data integrity and lease status."""
        try:
            # Check lease before storing
            if self.check_lease(filename):
                logging.warning("Cannot store chunk %s for file %s because it is currently leased.", chunk_id, filename)
                return {'status': 'error', 'message': 'File is currently leased'}

            os.makedirs(self.myChunkDir, exist_ok=True)
            path = os.path.join(self.myChunkDir, f"{filename}_{chunk_id}")
            
            if os.path.exists(path):
                logging.warning("Chunk %s already exists. Skipping storage.", chunk_id)
                return {'status': 'error', 'message': 'Chunk already exists'}

            # Verify checksum
            if self.calculate_checksum(data) != checksum:
                logging.error("Checksum mismatch for chunk %s, possible data corruption.", chunk_id)
                return {'status': 'error', 'message': 'Checksum mismatch'}

            with open(path, 'wb') as f:
                f.write(data)
            logging.info("Stored chunk %s successfully.", chunk_id)

            self.chunkserver_info.append((filename, chunk_id))
            self.connect_to_master(filename, chunk_id)
            return {'status': 'success'}
        except Exception as e:
            logging.error("Failed to store chunk %s: %s", chunk_id, e)
            return {'status': 'error', 'message': str(e)}

    def handle_request(self, client, address):
        """Handle client and chunk server requests."""
        try:
            request = pickle.loads(client.recv(4096))
            command = request.get('command')

            if command == 'store':
                filename = request['filename']
                chunk_id = request['chunk_id']
                data = request['data']
                checksum = request['checksum']
                response = self.store_chunk(client, chunk_id, filename, data, checksum)
                client.send(pickle.dumps(response))

            elif command == 'download':
                filename = request['filename']
                chunk_id = request['chunk_id']
                response = self.send_chunk(client, chunk_id, filename)
                client.send(pickle.dumps(response))

            elif command == 'replicate':
                filename = request['filename']
                chunk_id = request['chunk_id']
                data = request['data']
                checksum = request['checksum']
                response = self.store_chunk(client, chunk_id, filename, data, checksum)
                client.send(pickle.dumps(response))

            client.close()
        except Exception as e:
            logging.error("Error handling request from %s: %s", address, e)
        finally:
            client.close()

    def send_chunk(self, client, chunk_id, filename):
        """Send the requested chunk to client, including checksum for verification."""
        try:
            path = os.path.join(self.myChunkDir, f"{filename}_{chunk_id}")
            with open(path, 'rb') as f:
                data = f.read()
                checksum = self.calculate_checksum(data)
                return {'status': 'success', 'data': data, 'checksum': checksum}
        except FileNotFoundError:
            logging.error("Requested chunk %s not found", chunk_id)
            return {'status': 'error', 'message': 'Chunk not found'}
        except Exception as e:
            logging.error("Failed to send chunk %s: %s", chunk_id, e)
            return {'status': 'error', 'message': str(e)}

if __name__ == "__main__":
    try:
        port_num = int(sys.argv[1])
        filesystem = os.path.join(os.getcwd(), str(port_num - 6466))  # Generates unique directory for each server
        chunk_server = ChunkServer('localhost', port_num, filesystem, filesystem)
        logging.info("Starting Chunk Server on port %d", port_num)
        chunk_server.start()
    except Exception as e:
        logging.critical("Failed to start chunk server: %s", e)
        sys.exit(1)
