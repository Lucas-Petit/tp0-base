import socket
import logging


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True

    def run(self):
        """
        Server main loop that gracefully handles shutdown signals
        """
        try:
            while self._running:
                try:
                    # Set a timeout to allow periodic checking of _running flag
                    self._server_socket.settimeout(1.0)
                    client_sock = self.__accept_new_connection()
                    self.__handle_client_connection(client_sock)
                except socket.timeout:
                    # This is expected, just continue the loop to check _running flag
                    continue
                except OSError as e:
                    if self._running:
                        logging.error(f"action: accept_connections | result: fail | error: {e}")
                    else:
                        # Server is shutting down, this is expected
                        break
        finally:
            self._cleanup()
    
    def stop(self):
        """
        Gracefully stop the server
        """
        logging.info("action: server_shutdown_started | result: success | msg: Stopping server")
        self._running = False
        
    def _cleanup(self):
        """
        Clean up server resources
        """
        if hasattr(self, '_server_socket'):
            logging.info("action: closing_server_socket | result: success | msg: Server socket closed")
            self._server_socket.close()
        logging.info("action: server_shutdown_completed | result: success | msg: Server shutdown completed")

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        addr = None
        try:
            addr = client_sock.getpeername()
            # TODO: Modify the receive to avoid short-reads
            msg = client_sock.recv(1024).rstrip().decode('utf-8')
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(msg).encode('utf-8'))
        except OSError as e:
            if addr:
                logging.error(f"action: receive_message | result: fail | ip: {addr[0]} | error: {e}")
            else:
                logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            if addr:
                logging.info(f"action: closing_client_connection | result: success | ip: {addr[0]}")
            else:
                logging.info("action: closing_client_connection | result: success")
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
