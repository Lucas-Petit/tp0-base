import socket
from typing import Any, Optional


class MessageType:
    BET = "BET"
    RESPONSE = "RESPONSE"


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


class Message:
    def __init__(self, msg_type: str, data: Any):
        self.type = msg_type
        self.data = data


def serialize_bet(bet: Bet) -> str:
    """Convert a bet to our custom protocol format"""
    return f"{bet.agency}|{bet.first_name}|{bet.last_name}|{bet.document}|{bet.birthdate}|{bet.number}"


def deserialize_bet(data: str) -> Bet:
    """Convert our custom protocol format to a bet"""
    parts = data.split("|")
    if len(parts) != 6:
        raise ValueError("Invalid bet format")
    return Bet(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])


def serialize_response(response: Response) -> str:
    """Convert a response to our custom protocol format"""
    success_str = "true" if response.success else "false"
    return f"{success_str}|{response.message}"


def deserialize_response(data: str) -> Response:
    """Convert our custom protocol format to a response"""
    parts = data.split("|")
    if len(parts) != 2:
        raise ValueError("Invalid response format")
    success = parts[0] == "true"
    return Response(success, parts[1])


def send_message(conn: socket.socket, message: Message) -> bool:
    """
    Send a message over a connection using custom binary protocol.
    Returns True if successful, False otherwise.
    """
    try:
        if message.type == MessageType.BET:
            if isinstance(message.data, Bet):
                payload = serialize_bet(message.data)
            else:
                return False
        elif message.type == MessageType.RESPONSE:
            if isinstance(message.data, Response):
                payload = serialize_response(message.data)
            else:
                return False
        else:
            return False
        
        # Format: [TYPE___][LENGTH][PAYLOAD]\n
        msg_type = f"{message.type:<8}"
        length = f"{len(payload):010d}"
        full_message = (msg_type + length + payload + "\n").encode('utf-8')
        
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
    Receive a message from a connection using custom binary protocol.
    Returns Message if successful, None otherwise.
    """
    try:
        type_data = b''
        while len(type_data) < 8:
            chunk = conn.recv(8 - len(type_data))
            if not chunk:
                return None
            type_data += chunk
        
        msg_type = type_data.decode('utf-8').strip()
        
        length_data = b''
        while len(length_data) < 10:
            chunk = conn.recv(10 - len(length_data))
            if not chunk:
                return None
            length_data += chunk
        
        payload_length = int(length_data.decode('utf-8'))
        
        payload_data = b''
        while len(payload_data) < payload_length + 1:
            chunk = conn.recv((payload_length + 1) - len(payload_data))
            if not chunk:
                return None
            payload_data += chunk
        
        payload = payload_data[:-1].decode('utf-8')
        
        if msg_type == MessageType.BET:
            data = deserialize_bet(payload)
        elif msg_type == MessageType.RESPONSE:
            data = deserialize_response(payload)
        else:
            return None
        
        return Message(msg_type, data)
        
    except Exception:
        return None
