package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/harshithgowda/distributed-key-value-store/pkg/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: client <server1,server2,server3>")
		os.Exit(1)
	}

	servers := strings.Split(os.Args[1], ",")
	kvClient := client.NewClient(servers)

	scanner := bufio.NewScanner(os.Stdin)
	
	fmt.Println("Distributed Key-Value Store Client")
	fmt.Println("Commands: get <key>, put <key> <value>, delete <key>, getall, status <server>, exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])

		switch command {
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			
			value, err := kvClient.Get(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s = %s\n", parts[1], value)
			}

		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			
			err := kvClient.Put(parts[1], parts[2])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			
			err := kvClient.Delete(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "getall":
			data, err := kvClient.GetAll()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				if len(data) == 0 {
					fmt.Println("No data")
				} else {
					for k, v := range data {
						fmt.Printf("%s = %s\n", k, v)
					}
				}
			}

		case "status":
			if len(parts) != 2 {
				fmt.Println("Usage: status <server>")
				continue
			}
			
			status, err := kvClient.GetStatus(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				for k, v := range status {
					fmt.Printf("%s: %v\n", k, v)
				}
			}

		case "exit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Println("Unknown command. Available commands: get, put, delete, getall, status, exit")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}