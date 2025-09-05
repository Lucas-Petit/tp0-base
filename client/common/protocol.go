package common

import (
	"fmt"
	"io"
	"net"
	"strings"
)

const (
	BATCH    = 1
	RESPONSE = 2
	FINISHED_NOTIFICATION = 3
	WINNERS_QUERY = 4
	WINNERS_RESPONSE = 5
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

type Batch struct {
	Agency string
	Bets   []Bet
}

type Response struct {
	Success bool
	Message string
}

type WinnersQuery struct {
	Agency string
}

type WinnersResponse struct {
	Winners []string
}

type FinishedNotification struct {
	Agency string
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

func SerializeBatch(batch Batch) []byte {
	var betStrings []string
	for _, bet := range batch.Bets {
		betStrings = append(betStrings, string(SerializeBet(bet)))
	}
	data := fmt.Sprintf("%s||%s", batch.Agency, strings.Join(betStrings, ";;"))
	return []byte(data)
}

func SerializeFinishedNotification(notification FinishedNotification) []byte {
	return []byte(notification.Agency)
}

func SerializeWinnersQuery(query WinnersQuery) []byte {
	return []byte(query.Agency)
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

func DeserializeWinnersResponse(data []byte) (*WinnersResponse, error) {
	dataStr := string(data)
	if dataStr == "" {
		return &WinnersResponse{
			Winners: []string{},
		}, nil
	}
	winners := strings.Split(dataStr, "|")
	return &WinnersResponse{
		Winners: winners,
	}, nil
}

func SendMessage(conn net.Conn, msg Message) error {
	var payload []byte
	switch msg.Type {
	case BATCH:
		batch, ok := msg.Data.(Batch)
		if !ok {
			return fmt.Errorf("invalid batch data")
		}
		payload = SerializeBatch(batch)
	case WINNERS_QUERY:
		query, ok := msg.Data.(WinnersQuery)
		if !ok {
			return fmt.Errorf("invalid winners query data")
		}
		payload = SerializeWinnersQuery(query)
	case FINISHED_NOTIFICATION:
		notification, ok := msg.Data.(FinishedNotification)
		if !ok {
			return fmt.Errorf("invalid finished notification data")
		}
		payload = SerializeFinishedNotification(notification)
	default:
		return fmt.Errorf("invalid message type")
	}
	
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
    totalRead := 0
    for totalRead < 1 {
        n, err := conn.Read(typeBytes[totalRead:])
        if err != nil {
            if err == io.EOF {
                return nil, fmt.Errorf("connection closed")
            }
            return nil, fmt.Errorf("failed to read message type: %v", err)
        }
        totalRead += n
    }
    
    messageType := int(typeBytes[0])
    
    lengthBytes := make([]byte, 4)
    totalRead = 0
    for totalRead < 4 {
        n, err := conn.Read(lengthBytes[totalRead:])
        if err != nil {
            return nil, fmt.Errorf("failed to read length: %v", err)
        }
        totalRead += n
    }
    
    dataLength := bytesToInt(lengthBytes)
    
    payload := make([]byte, dataLength)
    totalRead = 0
    for totalRead < dataLength {
        n, err := conn.Read(payload[totalRead:])
        if err != nil {
            return nil, fmt.Errorf("failed to read payload: %v", err)
        }
        totalRead += n
    }
    
    if messageType == RESPONSE {
        data, err := DeserializeResponse(payload)
        if err != nil {
            return nil, fmt.Errorf("failed to deserialize response: %v", err)
        }
        return &Message{Type: messageType, Data: data}, nil
    } else if messageType == WINNERS_RESPONSE {
        data, err := DeserializeWinnersResponse(payload)
        if err != nil {
            return nil, fmt.Errorf("failed to deserialize winners response: %v", err)
        }
        return &Message{Type: messageType, Data: data}, nil
    }

    return nil, fmt.Errorf("unexpected message type: %d", messageType)
}
