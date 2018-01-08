package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"log"
	"bufio"
	"regexp"
	"net"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]

	advertisersIps := readIpList("ip-blacklist.txt")
	readPackets(broker, group, topics, advertisersIps)

}

func readPackets(broker string, group string, topics []string, ip4Set map[[4]byte]bool) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true

	var bytes = 0

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:
				bytes += processMessage(*e, ip4Set)
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}

	fmt.Printf("Closing consumer.\n")
	fmt.Printf("Got %d bytes to and from the blacklist.\n", bytes)
	c.Close()
}

func processMessage(msg kafka.Message, ip4Set map[[4]byte]bool) int {
	// parse the whole packet: may be costly
	// pkt := gopacket.NewPacket(msg.Value, layers.LayerTypeEthernet, gopacket.Lazy)

	// Only get what we need: less costly, less allocations.
	// TODO: allocate outside of the kafka listening loop ;)
	var eth layers.Ethernet
	var ip4 layers.IPv4
	var ip6 layers.IPv6
	var udp layers.UDP
	var dnsreq layers.DNSQuestion

	parser := gopacket.NewDecodingLayerParser(layers.LayerTypeEthernet, &eth, &ip4, &ip6, &udp, &dnsreq)
	decoded := []gopacket.LayerType{}

	parser.DecodeLayers(msg.Value, &decoded)

	for _, layerType := range decoded {
		switch layerType {
		case layers.LayerTypeIPv6:
			// Do nothing: current list to check against only contains IPV4 addresses.
		case layers.LayerTypeIPv4:
			if checkIp4(ip4, ip4Set) {
				// In the list: count the bytes
				return len(msg.Value)
			}
		case layers.LayerTypeDNS:
			log.Printf("DNS Request from %+v at %+v: %+v", ip4.SrcIP.To4(), msg.Timestamp, string(dnsreq.Name))
		}
	}

	return 0
}

// Golang has no native sets... use a map of 4 bytes to bool instead
// TODO: only do a lookup of the non-local address. (Ie, check if the first byte is 192)
func checkIp4(ip4 layers.IPv4, ip4Set map[[4]byte]bool) bool {

	var addr [4]byte
	copy(addr[:], ip4.DstIP[0:4])

	_, present := ip4Set[addr]

	if present {
		return true
	}

	copy(addr[:], ip4.SrcIP[0:4])

	_, present = ip4Set[addr]

	return present
}

/**
 * Read a text file containing one ip (v4) per line and convert it to a set of 4-bytes arrays.
 * Will ignore anything not looking like an IP on a line.
 */
func readIpList(path string) map[[4]byte]bool {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	blacklist := make(map[[4]byte]bool)
	var ip = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if ip.MatchString(line) {
			blacklist[toIpBytes(line)] = true
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return blacklist
}

func toIpBytes(str string) [4]byte {
	var array [4]byte
	copy(array[:], net.ParseIP(str).To4()[0:4])
	return array
}
