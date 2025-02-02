import http.server
import socketserver
from multiprocessing import Process
import time

def start_server(port=8000):
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        print(f"Serving HTTP on port {port}...")
        httpd.serve_forever()

# Start the server in a detached process
def run_detached_server():
    server_process = Process(target=start_server, args=(8000,))
    server_process.daemon = True  # Ensures the server stops when the main program exits
    server_process.start()
    print(f"Server started in detached mode (PID: {server_process.pid})")
    return server_process

# Example usage
server_process = run_detached_server()

# Main program continues running
print("Main program is running...")
time.sleep(10)  # Simulate doing other work

# Stop the server process
print("Stopping server...")
server_process.terminate()
server_process.join()
print("Server stopped.")
