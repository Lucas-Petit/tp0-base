package common

import (
	"net"
	"os"
	"time"
	"bufio"
	"strings"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMaxAmount int
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
	conn    net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
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

// StartClientLoop loops sending bets by reading CSV file in batches
func (c *Client) StartClientLoop(sigChan <-chan os.Signal) {
	csvPath := fmt.Sprintf("/data/agency-%s.csv", c.config.ID)
	
	file, err := os.Open(csvPath)
	if err != nil {
		log.Errorf("action: open_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	batchNumber := 0
	lineNumber := 0

	log.Infof("action: start_batch_processing | result: success | client_id: %v | batch_size: %d", 
		c.config.ID, c.config.BatchMaxAmount)

	for {
		select {
		case sig := <-sigChan:
			log.Infof("action: signal_received | result: success | signal: %v | client_id: %v | msg: Starting graceful shutdown", sig, c.config.ID)
			log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
			return
		default:
		}

		batch := c.readNextBatch(scanner, &lineNumber)
		if len(batch) == 0 {
			break
		}

		batchNumber++

		log.Infof("action: preparing_batch | result: success | client_id: %v | batch_number: %d | batch_size: %d", 
			c.config.ID, batchNumber, len(batch))

		if err := c.createClientSocket(); err != nil {
			log.Errorf("action: create_connection | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		if err := c.sendBatch(batch); err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | batch_number: %d | error: %v", c.config.ID, batchNumber, err)
			c.conn.Close()
			c.conn = nil
			return
		}

		log.Infof("action: closing_connection | result: success | client_id: %v | batch_number: %d", c.config.ID, batchNumber)
		c.conn.Close()
		c.conn = nil

		select {
		case sig := <-sigChan:
			log.Infof("action: signal_received | result: success | signal: %v | client_id: %v | msg: Graceful shutdown during sleep", sig, c.config.ID)
			log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
		}
	}

	log.Infof("action: all_batches_completed | result: success | client_id: %v | total_batches_sent: %d", c.config.ID, batchNumber)
}

func (c *Client) readNextBatch(scanner *bufio.Scanner, lineNumber *int) []Bet {
	var batch []Bet
	
	for len(batch) < c.config.BatchMaxAmount && scanner.Scan() {
		*lineNumber++
		line := strings.TrimSpace(scanner.Text())
		
		if line == "" {
			continue
		}
		
		fields := strings.Split(line, ",")
		if len(fields) != 5 {
			log.Errorf("action: parse_csv_line | result: fail | line: %d | error: expected 5 fields, got %d", 
				*lineNumber, len(fields))
			continue
		}
		
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
		
		bet := Bet{
			Agency:    c.config.ID,
			FirstName: fields[0],
			LastName:  fields[1],
			Document:  fields[2],
			Birthdate: fields[3],
			Number:    fields[4],
		}
		batch = append(batch, bet)
	}
	
	if err := scanner.Err(); err != nil {
		log.Errorf("action: read_batch | result: fail | error: %v", err)
	}
	
	return batch
}

func (c *Client) sendBatch(batch []Bet) error {
	batchData := Batch{
		Agency: c.config.ID,
		Bets:   batch,
	}
	message := Message{
		Type: BATCH,
		Data: batchData,
	}
	log.Infof("action: send_batch | result: success | client_id: %v | message_type: %d | batch_size: %d",
		c.config.ID, message.Type, len(batch))

	if err := SendMessage(c.conn, message); err != nil {
		return fmt.Errorf("failed to send batch: %v", err)
	}
	response, err := ReceiveMessage(c.conn)

	if err != nil {
		return fmt.Errorf("failed to receive response: %v", err)
	}

	if response.Type != RESPONSE {
		return fmt.Errorf("unexpected response type: %d", response.Type)
	}
	if resp, ok := response.Data.(*Response); ok {
		if !resp.Success {
			return fmt.Errorf("server error: %s", resp.Message)
		}
		log.Infof("action: batch_confirmed | result: success | client_id: %v | message: %s", 
			c.config.ID, resp.Message)
	} else {
		return fmt.Errorf("invalid response data format")
	}
	return nil
}