import glob
import socket
import logging
from .protocol import BATCH, FINISHED_NOTIFICATION, WINNERS_QUERY, WINNERS_RESPONSE, Bet, WinnersResponse, receive_message, send_message, Message, RESPONSE, Response
from .utils import has_won, load_bets, store_bets


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_sockets = []

        self._finished_agencies = set()
        self._lottery_completed = False
        self._total_agencies = self._count_agency_files()
        self._pending_winner_queries = []
        self._all_bets = []

    def _count_agency_files(self):
        """
        Count the number of agency CSV files to determine expected agencies
        """
        try:
            agency_files = glob.glob('/data/agency-*.csv')
            count = len(agency_files)
            logging.info(f"action: count_agency_files | result: success | count: {count}")
            return count
        except Exception as e:
            logging.error(f"action: count_agency_files | result: fail | error: {e}")
            return 0

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
        Handle lottery bet batch from a specific client socket and closes the socket

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
            if message.type != BATCH and message.type != WINNERS_QUERY and message.type != FINISHED_NOTIFICATION:
                logging.error(f"action: receive_message | result: fail | ip: {addr[0]} | error: Invalid message type")
                self.__send_error_response(client_sock, "Invalid message type")
                return

            if message.type == WINNERS_QUERY:
                self.__handle_winners_query(client_sock, message.data, addr)
            elif message.type == BATCH:
                batch_data = message.data
                self.__handle_batch_bets(client_sock, batch_data, addr)
            elif message.type == FINISHED_NOTIFICATION:
                self.__handle_finished_notification(client_sock, message.data, addr)

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
            self._client_sockets.remove(client_sock)

    def __handle_batch_bets(self, client_sock, batch_data, addr):
        """Handle a batch of bets message"""
        
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
            else:
                logging.info(f"action: send_response | result: success | ip: {addr[0]}")
            
        except Exception as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {bet_count}")
            self.__send_error_response(client_sock, f"Failed to store batch: {e}")
    
    def __handle_finished_notification(self, client_sock, finished_data, addr):
        """Handle a finished notification from an agency"""
        agency = finished_data.agency
        logging.info(f'action: receive_finished_notification | result: success | ip: {addr[0]} | agency: {agency}')

        try:
            if agency not in self._finished_agencies:
                self._finished_agencies.add(agency)
                logging.info(f"action: agency_finished | result: success | agency: {agency} | finished_count: {len(self._finished_agencies)}")
                
                if len(self._finished_agencies) >= self._total_agencies and not self._lottery_completed:
                    self.__perform_lottery()
            else:
                logging.info(f"action: agency_already_finished | result: success | agency: {agency} | finished_count: {len(self._finished_agencies)}")
            
            response = Response(success=True, message=f"Agency {agency} finished notification received")
            response_message = Message(RESPONSE, response)
            
            if not send_message(client_sock, response_message):
                logging.error(f"action: send_response | result: fail | ip: {addr[0]} | error: Failed to send response")
            else:
                logging.info(f"action: send_response | result: success | ip: {addr[0]}")
        
        except Exception as e:
            logging.error(f"action: handle_finished_notification | result: fail | ip: {addr[0]} | agency: {agency} | error: {e}")
            self.__send_error_response(client_sock, f"Failed to handle finished notification: {e}")

    def __handle_winners_query(self, client_sock, query_data, addr):
        """Handle a winners query from an agency"""
        agency = query_data.agency
        logging.info(f'action: receive_winners_query | result: success | ip: {addr[0]} | agency: {agency}')        

        try:
            if not self._lottery_completed:
                response = Response(success=False, message="Lottery not completed yet, please try again")
                response_message = Message(RESPONSE, response)
                
                if not send_message(client_sock, response_message):
                    logging.error(f"action: send_response | result: fail | ip: {addr[0]} | error: Failed to send response")
                else:
                    logging.info(f"action: send_response | result: success | ip: {addr[0]} | agency: {agency} | message: lottery_not_complete")
                return
            
            winners = self.__get_winners_for_agency(agency)
            
            winners_response = WinnersResponse(winners)
            response_message = Message(WINNERS_RESPONSE, winners_response)
            
            if not send_message(client_sock, response_message):
                logging.error(f"action: send_winners_response | result: fail | ip: {addr[0]} | error: Failed to send winners response")
            else:
                logging.info(f"action: send_winners_response | result: success | ip: {addr[0]} | agency: {agency} | winners_count: {len(winners)}")
            
        except Exception as e:
            logging.error(f"action: handle_winners_query | result: fail | ip: {addr[0]} | agency: {agency} | error: {e}")
            self.__send_error_response(client_sock, f"Failed to handle winners query: {e}")

    def __perform_lottery(self):
        """Perform the lottery when all agencies have finished"""
        try:
            logging.info("action: sorteo | result: success")
            self._lottery_completed = True
            self._all_bets = list(load_bets())

        except Exception as e:
            logging.error(f"action: sorteo | result: fail | error: {e}")

    def __get_winners_for_agency(self, agency):
        """Get list of winning DNI numbers for a specific agency"""
        winners = []
        try:
            if not self._all_bets:
                logging.error(f"action: get_winners_for_agency | result: fail | agency: {agency} | error: No bets loaded")
                return winners

            agency_int = int(agency)
            for bet in self._all_bets:
                if bet.agency == agency_int and has_won(bet):
                    winners.append(bet.document)
            
            logging.info(f"action: get_winners_for_agency | result: success | agency: {agency} | winners_found: {len(winners)}")
            
        except Exception as e:
            logging.error(f"action: get_winners_for_agency | result: fail | agency: {agency} | error: {e}")
        
        return winners

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
