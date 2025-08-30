package common

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

type BetData struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

type Client struct {
	config  ClientConfig
	betData BetData
	conn    net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter and reading bet data from environment variables
func NewClient(config ClientConfig) *Client {
	betData := BetData{
		FirstName: os.Getenv("NOMBRE"),
		LastName:  os.Getenv("APELLIDO"),
		Document:  os.Getenv("DOCUMENTO"),
		Birthdate: os.Getenv("NACIMIENTO"),
		Number:    os.Getenv("NUMERO"),
	}
	
	client := &Client{
		config:  config,
		betData: betData,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

// StartClientLoop loops sending bets
func (c *Client) StartClientLoop(ctx context.Context) {
	for i := 0; i < c.config.LoopAmount; i++ {
		// Check if context was cancelled (SIGTERM received)
		select {
		case <-ctx.Done():
			log.Infof("action: client_shutdown | result: success | client_id: %v | msg: Graceful shutdown initiated", c.config.ID)
			log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
			return
		default:
			// Continue with normal operation
		}

		if err := c.createClientSocket(); err != nil {
			return
		}
		
		c.sendSingleBet()
		
		log.Infof("action: closing_connection | result: success | client_id: %v", c.config.ID)
		c.conn.Close()
		c.conn = nil
		
		if i < c.config.LoopAmount-1 {
			select {
			case <-ctx.Done():
				log.Infof("action: client_shutdown | result: success | client_id: %v | msg: Graceful shutdown initiated", c.config.ID)
				log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
				return
			case <-time.After(c.config.LoopPeriod):
				// Continue to next iteration
			}
		}
	}
}

// sendSingleBet sends a single bet to the server
func (c *Client) sendSingleBet() {
	bet := Bet{
		Agency:    c.config.ID,
		FirstName: c.betData.FirstName,
		LastName:  c.betData.LastName,
		Document:  c.betData.Document,
		Birthdate: c.betData.Birthdate,
		Number:    c.betData.Number,
	}

	message := Message{
		Type: BetMessage,
		Data: bet,
	}

	if err := SendMessage(c.conn, message); err != nil {
		log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	response, err := ReceiveMessage(c.conn)
	if err != nil {
		log.Errorf("action: receive_response | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	responseData, ok := response.Data.(*Response)
	if !ok {
		log.Errorf("action: parse_response | result: fail | client_id: %v | error: invalid response type", c.config.ID)
		return
	}

	if responseData.Success {
		log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", 
			c.betData.Document, c.betData.Number)
	} else {
		log.Errorf("action: apuesta_enviada | result: fail | dni: %s | numero: %s | error: %s", 
			c.betData.Document, c.betData.Number, responseData.Message)
	}
}
