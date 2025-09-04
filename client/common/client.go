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

// StartClientLoop loops sending bets by reading CSV file in batches with persistent connection
func (c *Client) StartClientLoop(sigChan <-chan os.Signal) {
	csvPath := fmt.Sprintf("/data/agency-%s.csv", c.config.ID)
	
	file, err := os.Open(csvPath)
	if err != nil {
		log.Errorf("action: open_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer file.Close()

	// Create persistent connection at the start
	if err := c.createClientSocket(); err != nil {
		log.Errorf("action: create_persistent_connection | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	
	log.Infof("action: create_persistent_connection | result: success | client_id: %v", c.config.ID)
	
	// Ensure connection is closed when function exits
	defer func() {
		if c.conn != nil {
			log.Infof("action: closing_persistent_connection | result: success | client_id: %v", c.config.ID)
			c.conn.Close()
			c.conn = nil
		}
	}()

	scanner := bufio.NewScanner(file)
	batchNumber := 0
	lineNumber := 0

	log.Infof("action: start_batch_processing | result: success | client_id: %v | batch_size: %d", 
		c.config.ID, c.config.BatchMaxAmount)

	// Phase 1: Send all batches
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
			break // End of file - move to next phase
		}

		batchNumber++

		log.Infof("action: preparing_batch | result: success | client_id: %v | batch_number: %d | batch_size: %d", 
			c.config.ID, batchNumber, len(batch))

		if err := c.sendBatch(batch); err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | batch_number: %d | error: %v", c.config.ID, batchNumber, err)
			return
		}
	}

	// Phase 2: Send finished notification using same connection
	if err := c.notifyFinished(); err != nil {
		log.Errorf("action: notify_finished | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Phase 3: Query winners with retry logic using same connection
	if err := c.queryWinners(); err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
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

// notifyFinished sends a finished notification to the server
func (c *Client) notifyFinished() error {
	finishedNotification := FinishedNotification{
		Agency: c.config.ID,
	}
	
	message := Message{
		Type: FINISHED_NOTIFICATION,
		Data: finishedNotification,
	}
	
	log.Infof("action: notify_finished | result: success | client_id: %v", c.config.ID)

	if err := SendMessage(c.conn, message); err != nil {
		return fmt.Errorf("failed to send finished notification: %v", err)
	}

	response, err := ReceiveMessage(c.conn)
	if err != nil {
		return fmt.Errorf("failed to receive response: %v", err)
	}

	if response.Type != RESPONSE {
		return fmt.Errorf("unexpected response type: %s", response.Type)
	}

	if resp, ok := response.Data.(*Response); ok {
		if !resp.Success {
			return fmt.Errorf("server error: %s", resp.Message)
		}
		log.Infof("action: finished_notification_confirmed | result: success | client_id: %v | message: %s", 
			c.config.ID, resp.Message)
	} else {
		return fmt.Errorf("invalid response data type")
	}

	return nil
}

// queryWinners queries the server for winners of this agency
func (c *Client) queryWinners() error {
	log.Infof("action: query_winners | result: in_progress | client_id: %s", c.config.ID)

	query := WinnersQuery{Agency: c.config.ID}
	message := Message{Type: WINNERS_QUERY, Data: query}

	if err := SendMessage(c.conn, message); err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %s | error: failed to send query: %v", c.config.ID, err)
		return err
	}

	// Wait for the server to send the winners response when lottery is ready
	response, err := ReceiveMessage(c.conn)
	if err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %s | error: failed to receive response: %v", c.config.ID, err)
		return err
	}

	if response.Type == WINNERS_RESPONSE {
		winnersResponse := response.Data.(*WinnersResponse)
		winnerCount := len(winnersResponse.Winners)

		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", winnerCount)
		return nil
	} else {
		log.Errorf("action: query_winners | result: fail | client_id: %s | error: unexpected response type: %s", c.config.ID, response.Type)
		return fmt.Errorf("unexpected response type: %s", response.Type)
	}
}