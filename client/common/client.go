package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
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
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop(sigChan <-chan os.Signal) {
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		// Check if SIGTERM was received
		select {
		case sig := <-sigChan:
			log.Infof("action: signal_received | result: success | signal: %v | client_id: %v | msg: Starting graceful shutdown", sig, c.config.ID)
			if c.conn != nil {
				log.Infof("action: closing_connection | result: success | client_id: %v", c.config.ID)
				c.conn.Close()
			}
			log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
			return
		default:
			// Continue with normal operation
		}

		// Create the connection the server in every loop iteration. Send an
		c.createClientSocket()

		// TODO: Modify the send to avoid short-write
		fmt.Fprintf(
			c.conn,
			"[CLIENT %v] Message N°%v\n",
			c.config.ID,
			msgID,
		)
		msg, err := bufio.NewReader(c.conn).ReadString('\n')
		
		log.Infof("action: closing_connection | result: success | client_id: %v", c.config.ID)
		c.conn.Close()
		c.conn = nil

		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
			c.config.ID,
			msg,
		)

		// Wait a time between sending one message and the next one
		// Check for cancellation during sleep
		select {
		case sig := <-sigChan:
			log.Infof("action: signal_received | result: success | signal: %v | client_id: %v | msg: Graceful shutdown during sleep", sig, c.config.ID)
			log.Infof("action: client_shutdown_completed | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
			// Continue to next iteration
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
