
# Distributed File System (Inspired by GFS)

## Overview
A distributed file system based on the principles of the Google File System (GFS) with fault tolerance, chunk management, replication, and client-server interactions.

## Components
- **Master Server**: Manages metadata, chunk locations, client requests, and file leasing.
- **Chunk Servers**: Store and replicate chunks, respond to read/write/replicate requests, and send periodic heartbeats to the master.
- **Client Interface**: Provides file upload, download, listing, and leasing capabilities.

## Key Features
- **Chunk Management**: Files are split into fixed-size chunks (2048 bytes) and distributed across chunk servers.
- **Replication**: Each chunk is replicated (default factor: 2) for fault tolerance.
- **Integrity Checks**: Checksum validation prevents data corruption during storage and retrieval.
- **Heartbeat & Failure Detection**: Master server detects failed chunk servers and reallocates chunks.
- **File Operations**:
  - **Upload/Download**: Chunk-based file transfer with verification.
  - **List Files**: Retrieves available files from the master.
  - **Lease & Unlease**: Clients can lock files temporarily for exclusive write access.

## Project Structure
. ├── master_server.py # Manages metadata, chunk locations, leasing, and client requests ├── chunk_server.py # Stores chunks, handles replication, and sends heartbeats ├── client.py # Interface for file operations (upload, download, etc.) ├── master_server.log # Log for the master server ├── chunk_server.log # Logs for chunk servers ├── README.md # Project documentation


## Usage

### Running the System
1. **Master Server**: `python master_server.py`
2. **Chunk Servers** (multiple on different ports, e.g., 6467): `python chunk_server.py <PORT>`
3. **Client**: `python client.py`

### Client Commands
- **Upload**: `python client.py` > Menu > Select Upload
- **Download**: `python client.py` > Menu > Select Download
- **List Files**: `python client.py` > Menu > Select List Files
- **Lease/Unlease**: Manage file access locks for exclusive writing.


Future Work

    Improved error handling and fault tolerance.
    Enhanced scalability for distributed environments.
    Additional client operations like file deletion and metadata retrieval.
    Security improvements with authentication and encryption.
    
    
### Example Usage
```bash
# Start master server
python master_server.py

# Start chunk server on port 6467
python chunk_server.py 6467

# Run client for file operations
python client.py

