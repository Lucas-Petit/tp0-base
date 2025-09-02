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

// StartClientLoop loops sending bets
func (c *Client) StartClientLoop(sigChan <-chan os.Signal) {
	csvPath := fmt.Sprintf("/data/agency-%s.csv", c.config.ID)
	bets, err := c.readBetsFromCSV(csvPath)
	if err != nil {
		log.Errorf("action: read_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if len(bets) == 0 {
		log.Infof("action: read_bets | result: success | client_id: %v | msg: No bets found", c.config.ID)
		return
	}

	totalBatches := (len(bets) + c.config.BatchMaxAmount - 1) / c.config.BatchMaxAmount
	log.Infof("action: start_batch_processing | result: success | client_id: %v | total_bets: %d | batch_size: %d | total_batches: %d", 
		c.config.ID, len(bets), c.config.BatchMaxAmount, totalBatches)

	for i := 0; i < len(bets); i += c.config.BatchMaxAmount {
		// Check if SIGTERM was received
		select {
		case sig := <-sigChan:
			log.Infof("action: signal_received | result: success | signal: %v | client_id: %v | msg: Starting graceful shutdown", sig, c.config.ID)
			log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
			return
		default:
			// Continue with normal operation
		}

		end := i + c.config.BatchMaxAmount
		if end > len(bets) {
			end = len(bets)
		}
		batch := bets[i:end]
		batchNumber := (i / c.config.BatchMaxAmount) + 1

		log.Infof("action: preparing_batch | result: success | client_id: %v | batch_number: %d/%d | batch_size: %d", 
			c.config.ID, batchNumber, totalBatches, len(batch))

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

		if end < len(bets) {
			select {
			case sig := <-sigChan:
				log.Infof("action: signal_received | result: success | signal: %v | client_id: %v | msg: Graceful shutdown during sleep", sig, c.config.ID)
				log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
				return
			case <-time.After(c.config.LoopPeriod):
				// Continue to next batch
			}
		}
	}

	if err := c.notifyFinished(); err != nil {
		log.Errorf("action: notify_finished | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	
	if err := c.queryWinners(); err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: all_batches_completed | result: success | client_id: %v | total_batches_sent: %d", c.config.ID, totalBatches)
}

func (c *Client) readBetsFromCSV(csvPath string) ([]Bet, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	var bets []Bet
	scanner := bufio.NewScanner(file)
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) != 5 {
			log.Errorf("action: parse_csv_line | result: fail | line: %d | error: expected 5 fields, got %d", 
				lineNumber, len(fields))
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
		bets = append(bets, bet)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}
	return bets, nil
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
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	finishedNotification := FinishedNotification{
		Agency: c.config.ID,
	}
	
	message := Message{
		Type: FINISHED_NOTIFICATION,
		Data: finishedNotification,
	}
	
	log.Infof("action: notify_finished | result: success | client_id: %v", c.config.ID)

	if err := SendMessage(conn, message); err != nil {
		return fmt.Errorf("failed to send finished notification: %v", err)
	}

	response, err := ReceiveMessage(conn)
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
	maxRetries := 3
	retryDelay := time.Second * 2

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Infof("action: query_winners | result: in_progress | client_id: %s | attempt: %d", c.config.ID, attempt)

		conn, err := net.Dial("tcp", c.config.ServerAddress)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			log.Errorf("action: query_winners | result: fail | client_id: %s | error: failed to connect: %v", c.config.ID, err)
			return err
		}
		defer conn.Close()

		query := WinnersQuery{Agency: c.config.ID}
		message := Message{Type: WINNERS_QUERY, Data: query}

		if err := SendMessage(conn, message); err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			log.Errorf("action: query_winners | result: fail | client_id: %s | error: failed to send query: %v", c.config.ID, err)
			return err
		}

		response, err := ReceiveMessage(conn)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			log.Errorf("action: query_winners | result: fail | client_id: %s | error: failed to receive response: %v", c.config.ID, err)
			return err
		}

		if response.Type == WINNERS_RESPONSE {
			winnersResponse := response.Data.(*WinnersResponse)
			winnerCount := len(winnersResponse.Winners)

			log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", winnerCount)
			return nil
		} else if response.Type == RESPONSE {
			responseData := response.Data.(*Response)
			if !responseData.Success {
				if attempt < maxRetries {
					time.Sleep(retryDelay)
					continue
				}
				log.Infof("action: query_winners | result: fail | client_id: %s | message: %s", c.config.ID, responseData.Message)
				return fmt.Errorf("lottery not completed: %s", responseData.Message)
			}
		}

		if attempt < maxRetries {
			time.Sleep(retryDelay)
			continue
		}
		log.Errorf("action: query_winners | result: fail | client_id: %s | error: unexpected response type", c.config.ID)
		return fmt.Errorf("unexpected response type")
	}

	return fmt.Errorf("failed to query winners after %d attempts", maxRetries)
}