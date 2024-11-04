import socket
import threading
import os
import math
import pickle
import time
import logging

logging.basicConfig(filename='master_server.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

CHUNK_PORTS = [6467, 6468, 6469, 6470]
REPLICATION_FACTOR = 2
HEARTBEAT_INTERVAL = 5
LEASE_DURATION = 30  # Lease duration in seconds

class MasterServer:
    def __init__(self, host, port):
        self.chunksize = 2048
        self.host = host
        self.port = port
        self.file_map = {}  # Maps filenames to their chunk information
        self.chunk_locations = {}  # Maps chunk IDs to their respective chunk servers
        self.chunk_servers_info = {p: [] for p in CHUNK_PORTS}  # Tracks chunks held by each server
        self.active_servers = set()  # Set of active chunk servers for quick access
        self.leases = {}  # Tracks leases: {'filename': {'expires': <time>, 'client': <client_address>}}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        logging.info("Master Server initialized on host %s, port %d", host, port)

    def start(self):
        """Start the master server and begin listening for clients and chunk servers."""
        self.sock.listen(5)
        threading.Thread(target=self.heartbeat).start()
        threading.Thread(target=self.check_replication_integrity).start()
        threading.Thread(target=self.lease_expiration_checker).start()  # Check leases periodically
        logging.info("Master Server started, listening for connections.")
        while True:
            client, address = self.sock.accept()
            threading.Thread(target=self.handle_client, args=(client, address)).start()

    def num_chunks(self, size):
        return math.ceil(size / self.chunksize)

    def handle_client(self, client, address):
        """Handle incoming client requests."""
        try:
            request = pickle.loads(client.recv(4096))
            command = request.get('command')

            if command == 'upload':
                filename = request['filename']
                file_size = request['file_size']
                response = self.handle_upload(filename, file_size)
                client.send(pickle.dumps(response))

            elif command == 'download':
                filename = request['filename']
                response = self.get_chunk_locations(filename)
                client.send(pickle.dumps(response))

            elif command == 'list_files':
                response = list(self.file_map.keys())
                client.send(pickle.dumps(response))

            elif command == 'lease':
                filename = request['filename']
                response = self.lease_file(filename, address)
                client.send(pickle.dumps(response))

            elif command == 'unlease':
                filename = request['filename']
                response = self.unlease_file(filename)
                client.send(pickle.dumps(response))

            elif command == 'heartbeat':
                port = request['port']
                self.update_server_status(port)

            client.close()
        except Exception as e:
            logging.error("Error handling client request from %s: %s", address, e)

    def handle_upload(self, filename, file_size):
        """Handle file upload requests by allocating chunks and assigning servers."""
        if filename in self.file_map:
            return {'status': 'error', 'message': 'File already exists'}

        num_chunks = self.num_chunks(file_size)
        chunk_ids = [f"{filename}_chunk_{i}" for i in range(num_chunks)]
        self.file_map[filename] = chunk_ids

        # Allocate chunks to servers
        chunk_allocation = self.allocate_chunks(chunk_ids)
        return {'status': 'success', 'chunks': chunk_allocation}

    def get_chunk_locations(self, filename):
        """Return chunk locations for a requested file."""
        if filename not in self.file_map:
            return {'status': 'error', 'message': 'File not found'}

        chunk_locations = {chunk_id: self.chunk_locations.get(chunk_id, []) for chunk_id in self.file_map[filename]}
        return {'status': 'success', 'chunk_locations': chunk_locations}

    def lease_file(self, filename, client_address):
        """Lease a file to a client for exclusive write access."""
        if filename in self.leases and self.leases[filename]['expires'] > time.time():
            return {'status': 'error', 'message': f'File {filename} is already leased.'}

        # Grant lease and set expiration time
        self.leases[filename] = {
            'expires': time.time() + LEASE_DURATION,
            'client': client_address
        }
        logging.info("Leased file %s to client %s for %d seconds", filename, client_address, LEASE_DURATION)
        return {'status': 'success', 'message': f'File {filename} leased for {LEASE_DURATION} seconds.'}

    def unlease_file(self, filename):
        """Release a lease on a file, allowing other clients to access it."""
        if filename in self.leases:
            del self.leases[filename]
            logging.info("Unleased file %s", filename)
            return {'status': 'success', 'message': f'File {filename} has been unleased.'}
        else:
            return {'status': 'error', 'message': f'File {filename} was not leased.'}

    def lease_expiration_checker(self):
        """Periodically check and expire leases that have timed out."""
        while True:
            time.sleep(5)  # Check every 5 seconds
            current_time = time.time()
            expired_leases = [file for file, lease in self.leases.items() if lease['expires'] < current_time]
            for filename in expired_leases:
                del self.leases[filename]
                logging.info("Lease expired for file %s", filename)

    def allocate_chunks(self, chunk_ids):
        """Allocate chunks across available chunk servers with replication."""
        chunk_allocation = {}
        for chunk_id in chunk_ids:
            servers = self.select_chunk_servers(REPLICATION_FACTOR)
            self.chunk_locations[chunk_id] = servers

            # Track chunk assignments for each server
            for server in servers:
                self.chunk_servers_info[server].append(chunk_id)

            chunk_allocation[chunk_id] = servers
        return chunk_allocation

    def select_chunk_servers(self, replication_factor):
        """Select servers for chunk replication based on their current load and active status."""
        active_servers_info = {s: self.chunk_servers_info[s] for s in self.active_servers}
        available_servers = sorted(active_servers_info.items(), key=lambda x: len(x[1]))
        selected_servers = [server for server, _ in available_servers[:replication_factor]]

        if len(selected_servers) < replication_factor:
            logging.warning("Not enough active servers for full replication.")
        
        return selected_servers

    def update_server_status(self, port):
        """Update active server list based on heartbeat signals."""
        if port not in self.active_servers:
            self.active_servers.add(port)
            logging.info("Server on port %d is now active", port)

    def heartbeat(self):
        """Check active status of all chunk servers periodically."""
        logging.info("Heartbeat check initiated.")
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            inactive_servers = set(CHUNK_PORTS) - self.active_servers
            for port in inactive_servers:
                self.handle_server_failure(port)
            self.active_servers.clear()  # Reset active status for next interval

    def handle_server_failure(self, port):
        """Handle chunk server failure by reallocating chunks."""
        logging.warning("Chunk server on port %d has failed", port)
        if port in self.chunk_servers_info:
            for chunk_id in self.chunk_servers_info[port]:
                # Reallocate the failed chunk to another active server
                self.reallocate_chunk(chunk_id, port)

            # Clear failed server's data
            self.chunk_servers_info[port] = []

    def reallocate_chunk(self, chunk_id, failed_server):
        """Reallocate chunk replicas when a server goes down."""
        # Remove the failed server from chunk locations
        if chunk_id in self.chunk_locations:
            self.chunk_locations[chunk_id] = [s for s in self.chunk_locations[chunk_id] if s != failed_server]
            
            # Add a new replica if replication factor is not met
            if len(self.chunk_locations[chunk_id]) < REPLICATION_FACTOR:
                new_server = self.select_chunk_servers(1)[0]
                self.chunk_locations[chunk_id].append(new_server)
                self.chunk_servers_info[new_server].append(chunk_id)
                logging.info("Reallocated chunk %s to server on port %d", chunk_id, new_server)

    def check_replication_integrity(self):
        """Periodically verify that each chunk has the correct replication level."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL * 3)
            for chunk_id, servers in self.chunk_locations.items():
                if len(servers) < REPLICATION_FACTOR:
                    logging.warning("Chunk %s under-replicated, current replicas: %s", chunk_id, servers)
                    self.reallocate_chunk(chunk_id, None)

    def listen_to_chunk_server(self, client, address, filename, chunk_no, recv_port):
        """Handle requests from chunk servers for chunk locations."""
        chunk_no = int(chunk_no)
        servers = self.chunk_locations.get(f"{filename}_chunk_{chunk_no}", [])
        for server in servers:
            if server != int(recv_port):
                client.send(pickle.dumps(server))
                return
        client.send(pickle.dumps(None))


if __name__ == "__main__":
    master = MasterServer('localhost', 7082)
    logging.info("Master Server Running on port 7082")
    master.start()
