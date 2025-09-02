package common

import (
	"fmt"
	"io"
	"net"
	"strings"
)

const (
	BET      = 1
	RESPONSE = 2
)

type Message struct {
	Type int
	Data interface{}
}

type Bet struct {
	Agency    string
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

type Response struct {
	Success bool
	Message string
}

func intToBytes(value int, size int) []byte {
	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte((value >> (i * 8)) & 0xFF)
	}
	return bytes
}

func bytesToInt(bytes []byte) int {
	result := 0
	for i := 0; i < len(bytes); i++ {
		result |= int(bytes[i]) << (i * 8)
	}
	return result
}

func SerializeBet(bet Bet) []byte {
	data := fmt.Sprintf("%s|%s|%s|%s|%s|%s", 
		bet.Agency, bet.FirstName, bet.LastName, bet.Document, bet.Birthdate, bet.Number)
	return []byte(data)
}

func DeserializeResponse(data []byte) (*Response, error) {
	dataStr := string(data)
	parts := strings.Split(dataStr, "|")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid response format")
	}
	success := parts[0] == "true"
	return &Response{
		Success: success,
		Message: parts[1],
	}, nil
}

func SendMessage(conn net.Conn, msg Message) error {
	if msg.Type != BET {
		return fmt.Errorf("client can only send BET messages")
	}
	
	bet, ok := msg.Data.(Bet)
	if !ok {
		return fmt.Errorf("invalid bet data")
	}
	
	payload := SerializeBet(bet)
	
	header := make([]byte, 5)
	header[0] = byte(msg.Type)
	
	dataLength := len(payload)
	lengthBytes := intToBytes(dataLength, 4)
	copy(header[1:5], lengthBytes)
	
	fullMessage := append(header, payload...)
	
	totalWritten := 0
	for totalWritten < len(fullMessage) {
		n, err := conn.Write(fullMessage[totalWritten:])
		if err != nil {
			return fmt.Errorf("failed to write message: %v", err)
		}
		totalWritten += n
	}
	
	return nil
}

func ReceiveMessage(conn net.Conn) (*Message, error) {
	typeBytes := make([]byte, 1)
	_, err := io.ReadFull(conn, typeBytes)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("connection closed")
		}
		return nil, fmt.Errorf("failed to read message type: %v", err)
	}
	
	messageType := int(typeBytes[0])
	
	lengthBytes := make([]byte, 4)
	_, err = io.ReadFull(conn, lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %v", err)
	}
	
	dataLength := bytesToInt(lengthBytes)
	
	payload := make([]byte, dataLength)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload: %v", err)
	}
	
	if messageType == RESPONSE {
		data, err := DeserializeResponse(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize response: %v", err)
		}
		return &Message{Type: messageType, Data: *data}, nil
	}
	
	return nil, fmt.Errorf("unexpected message type: %d", messageType)
}
