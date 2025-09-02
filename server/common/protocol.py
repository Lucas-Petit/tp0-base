import socket
from typing import Optional


BATCH = 1
RESPONSE = 2
FINISHED_NOTIFICATION = 3
WINNERS_QUERY = 4
WINNERS_RESPONSE = 5


class Bet:
    def __init__(self, agency: str, first_name: str, last_name: str, 
                 document: str, birthdate: str, number: str):
        self.agency = agency
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number


class Response:
    def __init__(self, success: bool, message: str = ""):
        self.success = success
        self.message = message

class Batch:
    def __init__(self, agency: str, bets: list[Bet]):
        self.agency = agency
        self.bets = bets

class FinishedNotification:
    def __init__(self, agency: str):
        self.agency = agency

class WinnersQuery:
    def __init__(self, agency: str):
        self.agency = agency

class WinnersResponse:
    def __init__(self, winners: list):
        self.winners = winners

class Message:
    def __init__(self, msg_type: int, data):
        self.type = msg_type
        self.data = data


def deserialize_bet(data: str) -> Bet:
    """Convert string data to a bet"""
    parts = data.split("|")
    if len(parts) != 6:
        raise ValueError("Invalid bet format")
    return Bet(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])

def deserialize_batch(data: bytes) -> Batch:
    """Convert binary data to a batch"""
    data_str = data.decode('utf-8')
    parts = data_str.split("||")
    if len(parts) != 2:
        raise ValueError("Invalid batch format")
    
    agency = parts[0]
    bets_data = parts[1]
    
    bets = []
    if bets_data:
        bet_strings = bets_data.split(";;")
        for bet_str in bet_strings:
            bet = deserialize_bet(bet_str)
            bets.append(bet)
    
    return Batch(agency, bets)

def deserialize_winners_query(data: bytes) -> WinnersQuery:
    """Convert binary data to a winners query"""
    agency = data.decode('utf-8')
    return WinnersQuery(agency)

def deserialize_finished_notification(data: bytes) -> FinishedNotification:
    """Convert binary data to a finished notification"""
    agency = data.decode('utf-8')
    return FinishedNotification(agency)

def serialize_response(response: Response) -> bytes:
    """Convert a response to binary format"""
    success_str = "true" if response.success else "false"
    data_str = f"{success_str}|{response.message}"
    return data_str.encode('utf-8')

def serialize_winners_response(response: WinnersResponse) -> bytes:
    """Convert a winners response to binary format"""
    winners_data = "|".join(response.winners)
    return f"{winners_data}".encode('utf-8')

def send_message(conn: socket.socket, message: Message) -> bool:
    """
    Send a message using manual binary protocol: [1 byte type][4 bytes length][payload]
    Returns True if successful, False otherwise.
    """
    try:
        if message.type == RESPONSE:
            if isinstance(message.data, Response):
                payload = serialize_response(message.data)
            else:
                return False
        elif message.type == WINNERS_RESPONSE:
            if isinstance(message.data, WinnersResponse):
                payload = serialize_winners_response(message.data)
            else:
                return False
        else:
            return False
        
        type_byte = message.type.to_bytes(1, byteorder='little')
        
        data_length = len(payload)
        length_bytes = data_length.to_bytes(4, byteorder='little')
        
        full_message = type_byte + length_bytes + payload
        
        total_sent = 0
        while total_sent < len(full_message):
            sent = conn.send(full_message[total_sent:])
            if sent == 0:
                return False
            total_sent += sent
            
        return True
    except Exception:
        return False


def receive_message(conn: socket.socket) -> Optional[Message]:
    """
    Receive a message using manual binary protocol: [1 byte type][4 bytes length][payload]
    Returns Message if successful, None otherwise.
    """
    try:
        type_data = b''
        while len(type_data) < 1:
            chunk = conn.recv(1 - len(type_data))
            if not chunk:
                return None
            type_data += chunk
        
        message_type = int.from_bytes(type_data, byteorder='little')
        
        length_data = b''
        while len(length_data) < 4:
            chunk = conn.recv(4 - len(length_data))
            if not chunk:
                return None
            length_data += chunk
        
        data_length = int.from_bytes(length_data, byteorder='little')
        
        payload = b''
        while len(payload) < data_length:
            chunk = conn.recv(data_length - len(payload))
            if not chunk:
                return None
            payload += chunk

        if message_type == BATCH:
            data = deserialize_batch(payload)
        elif message_type == WINNERS_QUERY:
            data = deserialize_winners_query(payload)
        elif message_type == FINISHED_NOTIFICATION:
            data = deserialize_finished_notification(payload)
        else:
            return None
        return Message(message_type, data)
    except Exception:
        return None
