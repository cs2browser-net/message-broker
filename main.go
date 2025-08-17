package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
)

type Broker struct {
	mu     sync.RWMutex
	topics map[string]map[string]map[net.Conn]bool
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]map[string]map[net.Conn]bool),
	}
}

func (b *Broker) Subscribe(topic, group string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.topics[topic] == nil {
		b.topics[topic] = make(map[string]map[net.Conn]bool)
	}
	if b.topics[topic][group] == nil {
		b.topics[topic][group] = make(map[net.Conn]bool)
	}
	b.topics[topic][group][conn] = true
	log.Printf("Client subscribed to %s (group: %s)", topic, group)
}

func (b *Broker) Publish(topic, msg string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	groups, ok := b.topics[topic]
	if !ok {
		return
	}

	log.Printf("[%s] %s", topic, msg)

	for group, conns := range groups {
		if group == "" {
			for conn := range conns {
				fmt.Fprintf(conn, "[%s] %s\n", topic, msg)
			}
		} else {
			var pool []net.Conn
			for conn := range conns {
				pool = append(pool, conn)
			}
			if len(pool) > 0 {
				chosen := pool[rand.Intn(len(pool))]
				fmt.Fprintf(chosen, "[%s] %s\n", topic, msg)
			}
		}
	}
}

func (b *Broker) UnsubscribeAll(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, groups := range b.topics {
		for group, conns := range groups {
			if _, ok := conns[conn]; ok {
				delete(conns, conn)
				log.Printf("Client unsubscribed from %s (group: %s)", topic, group)
			}
		}
	}
}

func handleConnection(conn net.Conn, broker *Broker) {
	defer func() {
		broker.UnsubscribeAll(conn)
		conn.Close()
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		input := scanner.Text()
		parts := strings.SplitN(input, " ", 3)

		if len(parts) < 2 {
			fmt.Fprintln(conn, "Invalid command")
			continue
		}

		cmd := strings.ToUpper(parts[0])
		topic := parts[1]

		switch cmd {
		case "SUB":
			group := ""
			if len(parts) == 3 {
				group = parts[2]
			}
			broker.Subscribe(topic, group, conn)
		case "PUB":
			if len(parts) < 3 {
				fmt.Fprintln(conn, "Missing message for PUB")
				continue
			}
			msg := parts[2]
			broker.Publish(topic, msg)
		default:
			fmt.Fprintln(conn, "Unknown command")
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", ":4222")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	log.Println("Pub/Sub server started on :4222")

	broker := NewBroker()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go handleConnection(conn, broker)
	}
}
