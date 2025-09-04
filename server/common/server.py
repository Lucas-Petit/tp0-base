import socket
import logging
from .protocol import BATCH, Bet, receive_message, send_message, Message, RESPONSE, Response
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
        Handle multiple lottery bet batches from a specific client socket until client disconnects

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        addr = None
        try:
            addr = client_sock.getpeername()
            
            while True:
                message = receive_message(client_sock)
                if not message:
                    logging.info(f"action: client_disconnected | result: success | ip: {addr[0]}")
                    break
                
                if message.type != BATCH:
                    logging.error(f"action: receive_message | result: fail | ip: {addr[0]} | error: Invalid message type")
                    self.__send_error_response(client_sock, "Invalid message type")
                    break
                
                batch_data = message.data
                if not self.__handle_batch_bets(client_sock, batch_data, addr):
                    break
                
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
            if client_sock in self._client_sockets:
                self._client_sockets.remove(client_sock)

    def __handle_batch_bets(self, client_sock, batch_data, addr):
        """Handle a batch of bets message. Returns True if successful."""
        
        bet_count = len(batch_data.bets)
        logging.info(f'action: receive_message | result: success | ip: {addr[0]} | cantidad: {bet_count}')
        
        try:
            bets = []
            for bet in batch_data.bets:
                bet = Bet(
                    agency=bet.agency,
                    first_name=bet.first_name,
                    last_name=bet.last_name,
                    document=bet.document,
                    birthdate=bet.birthdate,
                    number=bet.number
                )
                bets.append(bet)
            
            store_bets(bets)
            logging.info(f"action: apuesta_recibida | result: success | cantidad: {bet_count}")
            
            response = Response(success=True, message=f"Batch of {bet_count} bets stored successfully")
            response_message = Message(RESPONSE, response)
            
            if not send_message(client_sock, response_message):
                logging.error(f"action: send_response | result: fail | ip: {addr[0]} | error: Failed to send response")
                return False
            else:
                logging.info(f"action: send_response | result: success | ip: {addr[0]}")
                return True
            
        except Exception as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {bet_count}")
            self.__send_error_response(client_sock, f"Failed to store batch: {e}")
            return False
    
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
