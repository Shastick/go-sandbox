package main

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"log"
	"time"
	"github.com/google/gopacket/layers"
)

var (
	device       string = "en0"
	snapshot_len int32  = 1024
	promiscuous  bool   = false
	err          error
	timeout      time.Duration = 30 * time.Second
	handle       *pcap.Handle
)

func main() {
	// Open device
	handle, err = pcap.OpenLive(device, snapshot_len, promiscuous, timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	// Set filter
	var filter = "udp port 53"
	err = handle.SetBPFFilter(filter)
	if err != nil {
		log.Fatal(err)
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		// Do something with a packet here.

		if dnsLayer := packet.Layer(layers.LayerTypeDNS); dnsLayer != nil {
			fmt.Println("Got a DNS packet.")
			dns := dnsLayer.(*layers.DNS)

			if len(dns.Answers) == 0 {
				fmt.Println("It's a query: ")
				for _, q := range dns.Questions {
					fmt.Printf("\t %s from %v", string(q.Name), )
				}
			} else {
				fmt.Println("It's a reply:")
				for _, q := range dns.Questions {
					fmt.Printf("\t %s\n", string(q.Name))
				}
			}
		}
	}

}