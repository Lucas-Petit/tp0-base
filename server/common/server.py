import glob
import socket
import logging
import threading
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
        
        self._lock = threading.Lock()
        self._winner_queries_received = 0
        self._client_threads = []
        self._winners_sent = False
        
        self._finished_barrier = threading.Barrier(self._total_agencies)
        self._winners_barrier = threading.Barrier(self._total_agencies)

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
        Server main loop: accept all clients and spawn threads to handle them
        """
        while True:
            try:
                while len(self._client_sockets) < self._total_agencies:
                    client_sock = self.__accept_new_connection()
                    with self._lock:
                        self._client_sockets.append(client_sock)
                    
                    client_thread = threading.Thread(
                        target=self.__handle_client_connection,
                        args=(client_sock,)
                    )
                    self._client_threads.append(client_thread)
                    client_thread.start()
                
                logging.info(f"action: all_clients_connected | result: success | count: {len(self._client_sockets)}")
                
                for thread in self._client_threads:
                    thread.join()
                
                logging.info("action: all_client_threads_completed | result: success")
                
            except OSError as e:
                logging.error(f"action: server_main_loop | result: fail | error: {e}")
                self.stop()
                break
    
    def stop(self):
        """
        Gracefully stop the server
        """
        for sock in self._client_sockets:
            sock.close()
        self._client_sockets = []
        self._server_socket.close()
        for thread in self._client_threads:
            thread.join(timeout=5.0)
            if thread.is_alive():
                logging.warning("action: thread_join_timeout | result: fail | msg: Thread did not terminate in time")
        self._client_threads = []
        logging.info("action: server_shutdown_finished | result: success | msg: Stopping server")

    def __handle_client_connection(self, client_sock):
        """Handle all messages from a single client in a dedicated thread using barriers"""
        addr = None
        try:
            addr = client_sock.getpeername()
            logging.info(f"action: start_client_thread | result: success | ip: {addr[0]}")
            
            finished_received = False
            while not finished_received:
                try:
                    message = receive_message(client_sock)
                    if not message:
                        logging.error(f"action: client_disconnected_early | result: fail | ip: {addr[0]}")
                        return
                    
                    if message.type == FINISHED_NOTIFICATION:
                        self.__handle_finished_notification(client_sock, message.data, addr)
                        finished_received = True
                    else:
                        self.__handle_single_message(client_sock, message, addr)
                        
                except (ConnectionResetError, BrokenPipeError, OSError) as e:
                    logging.error(f"action: client_disconnected_during_batches | result: fail | ip: {addr[0]} | error: {e}")
                    return
            
            self._finished_barrier.wait()
            
            with self._lock:
                if not self._lottery_completed:
                    self._lottery_completed = True
                    self.__perform_lottery()
                    logging.info("action: performing_lottery_from_thread | result: success")
            
            try:
                message = receive_message(client_sock)
                if message and message.type == WINNERS_QUERY:
                    self.__handle_winners_query(client_sock, message.data, addr)
                else:
                    logging.error(f"action: expected_winner_query | result: fail | ip: {addr[0]} | received_type: {message.type if message else 'None'}")
                    
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                logging.error(f"action: client_disconnected_during_winner_query | result: fail | ip: {addr[0]} | error: {e}")
                return
            
            self._winners_barrier.wait()
            
            with self._lock:
                if not hasattr(self, '_winners_sent') or not self._winners_sent:
                    self._winners_sent = True
                    self.__send_winners_to_all_clients()
                    logging.info("action: sending_winners_from_thread | result: success")
                    
        except Exception as e:
            logging.error(f"action: handle_client_connection | result: fail | ip: {addr[0] if addr else 'unknown'} | error: {e}")
        finally:
            try:
                if client_sock in self._client_sockets:
                    with self._lock:
                        self._client_sockets.remove(client_sock)
                client_sock.close()
                logging.info(f"action: client_thread_finished | result: success | ip: {addr[0] if addr else 'unknown'}")
            except:
                pass

    def __handle_single_message(self, client_sock, message, addr):
        """Handle a single message from a client"""
        try:
            if message.type == BATCH:
                batch_data = message.data
                self.__handle_batch_bets(client_sock, batch_data, addr)
            elif message.type == FINISHED_NOTIFICATION:
                self.__handle_finished_notification(client_sock, message.data, addr)
            elif message.type == WINNERS_QUERY:
                self.__handle_winners_query(client_sock, message.data, addr)
            else:
                logging.error(f"action: handle_message | result: fail | ip: {addr[0]} | error: Invalid message type")
                self.__send_error_response(client_sock, "Invalid message type")
        except Exception as e:
            logging.error(f"action: handle_single_message | result: fail | ip: {addr[0]} | error: {e}")

    def __handle_winners_query(self, client_sock, query_data, addr):
        """Handle a winners query from an agency"""
        agency = query_data.agency
        logging.info(f'action: receive_winners_query | result: success | ip: {addr[0]} | agency: {agency}')

        try:
            with self._lock:
                self._winner_queries_received += 1
                lottery_completed = self._lottery_completed
                
                if not lottery_completed:
                    self._pending_winner_queries.append((client_sock, query_data, addr))
                    logging.info(f"action: store_pending_query | result: success | ip: {addr[0]} | agency: {agency} | count: {self._winner_queries_received}")
                    return
            
            winners = self.__get_winners_for_agency(agency)
            
            winners_response = WinnersResponse(winners)
            response_message = Message(WINNERS_RESPONSE, winners_response)
            
            if not send_message(client_sock, response_message):
                logging.error(f"action: send_immediate_winners_response | result: fail | ip: {addr[0]} | agency: {agency}")
            else:
                logging.info(f"action: send_immediate_winners_response | result: success | ip: {addr[0]} | agency: {agency} | winners_count: {len(winners)}")
                
        except Exception as e:
            logging.error(f"action: handle_winners_query | result: fail | ip: {addr[0]} | agency: {agency} | error: {e}")
            self.__send_error_response(client_sock, f"Failed to handle winners query: {e}")

    def __send_winners_to_all_clients(self):
        """Send winner responses to all clients that requested them"""
        if not self._pending_winner_queries:
            return
            
        logging.info(f"action: send_winners_to_all | result: success | pending_count: {len(self._pending_winner_queries)}")
        
        for client_sock, query_data, addr in self._pending_winner_queries:
            try:
                agency = query_data.agency
                winners = self.__get_winners_for_agency(agency)
                
                winners_response = WinnersResponse(winners)
                response_message = Message(WINNERS_RESPONSE, winners_response)
                
                if not send_message(client_sock, response_message):
                    logging.error(f"action: send_winners_response | result: fail | ip: {addr[0]} | agency: {agency}")
                else:
                    logging.info(f"action: send_winners_response | result: success | ip: {addr[0]} | agency: {agency} | winners_count: {len(winners)}")
                    
            except Exception as e:
                logging.error(f"action: send_winners_response | result: fail | ip: {addr[0]} | error: {e}")
        
        self._pending_winner_queries.clear()

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

            with self._lock:
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
            with self._lock:
                if agency not in self._finished_agencies:
                    self._finished_agencies.add(agency)
                    logging.info(f"action: agency_finished | result: success | agency: {agency} | finished_count: {len(self._finished_agencies)}")
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

    def __perform_lottery(self):
        """Perform the lottery when all agencies have finished"""
        try:
            self._all_bets = [bet for bet in load_bets() if has_won(bet)]
            
            logging.info(f"action: sorteo | result: success | total_winners: {len(self._all_bets)}")

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
                if bet.agency == agency_int:
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