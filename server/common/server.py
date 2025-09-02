import socket
import logging
from .protocol import receive_message, send_message, Message, BET, RESPONSE, Response
from .utils import store_bets


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
        Handle lottery bet from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        addr = None
        try:
            addr = client_sock.getpeername()
            
            message = receive_message(client_sock)
            if not message:
                logging.error(f"action: receive_message | result: fail | ip: {addr[0]} | error: Failed to receive message")
                self.__send_error_response(client_sock, "Failed to receive message")
                return
            
            if message.type != BET:
                logging.error(f"action: receive_message | result: fail | ip: {addr[0]} | error: Invalid message type")
                self.__send_error_response(client_sock, "Invalid message type")
                return
            
            bet_data = message.data
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | dni: {bet_data.document} | numero: {bet_data.number}')
            
            try:
                from .utils import Bet
                bet_utils = Bet(
                    agency=bet_data.agency,
                    first_name=bet_data.first_name,
                    last_name=bet_data.last_name,
                    document=bet_data.document,
                    birthdate=bet_data.birthdate,
                    number=bet_data.number
                )
                
                store_bets([bet_utils])
                
                logging.info(f"action: apuesta_almacenada | result: success | dni: {bet_data.document} | numero: {bet_data.number}")
                
                response = Response(success=True, message="Bet stored successfully")
                response_message = Message(RESPONSE, response)
                
                if not send_message(client_sock, response_message):
                    logging.error(f"action: send_response | result: fail | ip: {addr[0]} | error: Failed to send response")
                
            except Exception as e:
                logging.error(f"action: store_bet | result: fail | ip: {addr[0]} | dni: {bet_data.document if hasattr(bet_data, 'document') else 'unknown'} | error: {e}")
                self.__send_error_response(client_sock, f"Failed to store bet: {e}")
                
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
    
    def __send_error_response(self, client_sock, error_message):
        """Send an error response to the client"""
        try:
            response = Response(success=False, message=error_message)
            response_message = Message(RESPONSE, response)
            send_message(client_sock, response_message)
        except Exception:
            pass

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
