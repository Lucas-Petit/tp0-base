import socket
import logging
import sys


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_sockets = []

    def run(self):
        """
        Server main loop that gracefully handles shutdown signals
        """
        while True:
            try:
                client_sock = self.__accept_new_connection()
                self._client_sockets.append(client_sock)
                self.__handle_client_connection(client_sock)
            except OSError as e:
                logging.error(f"action: accept_connections | result: fail | error: {e}")
    
    def stop(self):
        """
        Gracefully stop the server
        """
        for sock in self._client_sockets:
            sock.close()
        self._client_sockets = []
        self._server_socket.close()
        logging.info("action: server_shutdown_finished | result: success | msg: Stopping server")

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
