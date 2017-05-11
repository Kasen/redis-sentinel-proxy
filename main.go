package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var (
	masterAddr *net.TCPAddr
	sentinels  []string

	localAddr     = flag.String("listen", ":9999", "local address")
	sentinelAddrs = flag.String("sentinel", ":26379", "List of remote address, separated by coma")
	masterName    = flag.String("master", "", "name of the master redis node")
	pidFile       = flag.String("pidfile", "", "Location of the pid file")
)

func main() {
	flag.Parse()

	if *pidFile != "" {
		f, err := os.Create(*pidFile)
		if err != nil {
			log.Fatalf("Unable to create pidfile: %s", err)
		}

		fmt.Fprintf(f, "%d\n", os.Getpid())

		f.Close()
	}

	sentinels = strings.Split(*sentinelAddrs, ",")

	laddr, err := net.ResolveTCPAddr("tcp", *localAddr)
	if err != nil {
		log.Fatalf("Failed to resolve local address: %s", err)
	}

	go master()

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Println(err)
			continue
		}

		go proxy(conn, masterAddr)
	}
}

func master() {
	resultChannel := make(chan *net.TCPAddr, 1)
	for _, sentinel := range sentinels {
		go getMasterAddr(sentinel, *masterName, resultChannel)
	}
	for {
		select {
		case result := <-resultChannel:
			if result != nil {
				masterAddr = result
			}
		case <-time.After(time.Second * 2):
			log.Println("All sentinels timed out.")
		}
	}
}

func pipe(r io.Reader, w io.WriteCloser) {
	io.Copy(w, r)
	w.Close()
}

func proxy(local io.ReadWriteCloser, remoteAddr *net.TCPAddr) {
	remote, err := net.DialTCP("tcp", nil, remoteAddr)
	if err != nil {
		log.Println(err)
		local.Close()
		return
	}
	go pipe(local, remote)
	go pipe(remote, local)
}

func getMasterAddr(sentinelAddress string, masterName string, resultChannel chan *net.TCPAddr) {
	for {
		conn, err := net.DialTimeout("tcp", sentinelAddress, 2*time.Second)
		if err != nil {
			log.Println(err)
			time.Sleep(2 * time.Second)
			continue
		}

		defer conn.Close()
		for {
			conn.Write([]byte(fmt.Sprintf("sentinel get-master-addr-by-name %s\n", masterName)))

			b := make([]byte, 256)
			_, err = conn.Read(b)
			if err != nil {
				log.Println(err)
				time.Sleep(2 * time.Second)
				break
			}

			parts := strings.Split(string(b), "\r\n")

			if len(parts) < 5 {
				err = fmt.Errorf("Couldn't get master address from sentinel: %s", string(b))
				log.Println(err)
				time.Sleep(2 * time.Second)
				break
			}

			//getting the string address for the master node
			stringaddr := fmt.Sprintf("%s:%s", parts[2], parts[4])
			addr, err := net.ResolveTCPAddr("tcp", stringaddr)
			if err != nil {
				log.Println(err)
				time.Sleep(2 * time.Second)
				break
			}

			resultChannel <- addr
			time.Sleep(time.Second)
		}
	}
}
