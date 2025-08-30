package common

import (
	"fmt"
	"io"
	"net"
	"strings"
)

type MessageType string

const (
	BetMessage     MessageType = "BET"
	ResponseMessage MessageType = "RESPONSE"
)

type Message struct {
	Type MessageType
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

func SerializeBet(bet Bet) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s", 
		bet.Agency, bet.FirstName, bet.LastName, bet.Document, bet.Birthdate, bet.Number)
}

func DeserializeBet(data string) (*Bet, error) {
	parts := strings.Split(data, "|")
	if len(parts) != 6 {
		return nil, fmt.Errorf("invalid bet format")
	}
	return &Bet{
		Agency:    parts[0],
		FirstName: parts[1],
		LastName:  parts[2],
		Document:  parts[3],
		Birthdate: parts[4],
		Number:    parts[5],
	}, nil
}

func SerializeResponse(resp Response) string {
	successStr := "false"
	if resp.Success {
		successStr = "true"
	}
	return fmt.Sprintf("%s|%s", successStr, resp.Message)
}

func DeserializeResponse(data string) (*Response, error) {
	parts := strings.Split(data, "|")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid response format")
	}
	success := parts[0] == "true"
	return &Response{
		Success: success,
		Message: parts[1],
	}, nil
}

// SendMessage sends a message over a connection using custom binary protocol
func SendMessage(conn net.Conn, msg Message) error {
	var payload string
	
	switch msg.Type {
	case BetMessage:
		if bet, ok := msg.Data.(Bet); ok {
			payload = SerializeBet(bet)
		} else {
			return fmt.Errorf("invalid bet data")
		}
	case ResponseMessage:
		if resp, ok := msg.Data.(Response); ok {
			payload = SerializeResponse(resp)
		} else {
			return fmt.Errorf("invalid response data")
		}
	default:
		return fmt.Errorf("unknown message type")
	}
	
	// Format: [TYPE___][LENGTH][PAYLOAD]\n
	msgType := fmt.Sprintf("%-8s", string(msg.Type))
	length := fmt.Sprintf("%010d", len(payload))
	fullMessage := msgType + length + payload + "\n"
	
	totalWritten := 0
	messageBytes := []byte(fullMessage)
	for totalWritten < len(messageBytes) {
		n, err := conn.Write(messageBytes[totalWritten:])
		if err != nil {
			return fmt.Errorf("failed to write message: %v", err)
		}
		totalWritten += n
	}
	
	return nil
}

// ReceiveMessage receives a message from a connection using custom binary protocol
func ReceiveMessage(conn net.Conn) (*Message, error) {
	typeBytes := make([]byte, 8)
	totalRead := 0
	for totalRead < 8 {
		n, err := conn.Read(typeBytes[totalRead:])
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("connection closed")
			}
			return nil, fmt.Errorf("failed to read message type: %v", err)
		}
		totalRead += n
	}
	
	msgType := MessageType(strings.TrimSpace(string(typeBytes)))
	
	lengthBytes := make([]byte, 10)
	totalRead = 0
	for totalRead < 10 {
		n, err := conn.Read(lengthBytes[totalRead:])
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("connection closed")
			}
			return nil, fmt.Errorf("failed to read length: %v", err)
		}
		totalRead += n
	}
	
	var payloadLength int
	if _, err := fmt.Sscanf(string(lengthBytes), "%010d", &payloadLength); err != nil {
		return nil, fmt.Errorf("failed to parse payload length: %v", err)
	}
	
	payloadBytes := make([]byte, payloadLength+1)
	totalRead = 0
	for totalRead < payloadLength+1 {
		n, err := conn.Read(payloadBytes[totalRead:])
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("connection closed while reading payload")
			}
			return nil, fmt.Errorf("failed to read payload: %v", err)
		}
		totalRead += n
	}
	
	payload := string(payloadBytes[:payloadLength])
	
	var data interface{}
	var err error
	
	switch msgType {
	case BetMessage:
		data, err = DeserializeBet(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize bet: %v", err)
		}
	case ResponseMessage:
		data, err = DeserializeResponse(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize response: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown message type: %s", msgType)
	}
	
	return &Message{
		Type: msgType,
		Data: data,
	}, nil
}
