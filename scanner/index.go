package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	DB           *sql.DB       = nil
	CURRENT_IP   net.IP        = net.IPv4(0, 0, 0, 0)
	CONCURRENCY  int           = 1
	CHANSIZE     int           = 30000
	DIAL_TIMEOUT time.Duration = 15 * time.Second
	Mu           sync.Mutex
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf(`
Usage: ftpscan [concurrency] [start ip]
`)
		return
	} else if err := setup(); err != nil {
		fmt.Printf("ERROR %s\n", err.Error())
		return
	} else if CURRENT_IP = net.ParseIP(os.Args[2]); CURRENT_IP == nil {
		fmt.Printf("ERROR %s\n", err.Error())
		return
	} else if n, err := strconv.Atoi(os.Args[1]); err == nil {
		CONCURRENCY = n
	}
	fmt.Printf("> concurrency: %d\n", CONCURRENCY)
	fmt.Printf("> start ip: %s\n", CURRENT_IP.String())
	queue := make(chan net.IP, CHANSIZE)
	var wg sync.WaitGroup
	for i := 0; i < CONCURRENCY; i++ {
		wg.Add(1)
		go func() {
			for ip := range queue {
				runner(ip)
			}
			wg.Done()
		}()
	}
	iterateThroughPublicIPs(queue)
	fmt.Printf("\n")
	close(queue)
	wg.Wait()
}

func setup() (err error) {
	DB, err = sql.Open("sqlite3", "./ftp.sqlite?_busy_timeout=5000&_journal_mode=DELETE")
	if err != nil {
		return err
	}
	if _, err = DB.Exec(`CREATE TABLE IF NOT EXISTS host (
  ip VARCHAR(32) PRIMARY KEY,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`); err != nil {
		return err
	} else if _, err = DB.Exec("PRAGMA synchronous = 0;"); err != nil {
		return err
	}
	DB.Exec("CREATE UNIQUE INDEX idx_ip ON host (ip);")
	return nil
}

func runner(ip net.IP) {
	if func(ip net.IP) bool {
		var PrivateIPNetworks = []net.IPNet{
			net.IPNet{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)},
			net.IPNet{IP: net.ParseIP("172.16.0.0"), Mask: net.CIDRMask(12, 32)},
			net.IPNet{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(16, 32)},
			// reports
			net.IPNet{IP: net.ParseIP("5.75.128.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("23.88.0.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("49.12.128.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("49.13.0.0"), Mask: net.CIDRMask(16, 32)},
			net.IPNet{IP: net.ParseIP("65.108.0.0"), Mask: net.CIDRMask(16, 32)},
			net.IPNet{IP: net.ParseIP("65.109.0.0"), Mask: net.CIDRMask(16, 32)},
			net.IPNet{IP: net.ParseIP("78.46.128.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("78.47.0.0"), Mask: net.CIDRMask(16, 32)},
			net.IPNet{IP: net.ParseIP("88.198.0.0"), Mask: net.CIDRMask(16, 32)},
			net.IPNet{IP: net.ParseIP("91.107.0.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("95.217.252.2"), Mask: net.CIDRMask(22, 32)},
			net.IPNet{IP: net.ParseIP("128.140.0.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("142.132.128.0"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("162.55.200.0"), Mask: net.CIDRMask(21, 32)},
			net.IPNet{IP: net.ParseIP("167.233.0.0"), Mask: net.CIDRMask(16, 32)},
			net.IPNet{IP: net.ParseIP("168.119.215.2"), Mask: net.CIDRMask(20, 32)},
			net.IPNet{IP: net.ParseIP("188.34.168.2"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("213.133.113.2"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("213.239.228.2"), Mask: net.CIDRMask(17, 32)},
			net.IPNet{IP: net.ParseIP("213.239.228.2"), Mask: net.CIDRMask(19, 32)},
		}
		for _, ipNet := range PrivateIPNetworks {
			if ipNet.Contains(ip) {
				return true
			}
		}
		return false
	}(ip) {
		return
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:21", ip.String()), DIAL_TIMEOUT)
	if err != nil {
		if strings.Contains(err.Error(), "i/o timeout") == false &&
			strings.Contains(err.Error(), "network is unreachable") == false &&
			strings.Contains(err.Error(), "connection refused") == false &&
			strings.Contains(err.Error(), "no route to host") == false &&
			strings.Contains(err.Error(), "connection reset by peer") == false &&
			strings.Contains(err.Error(), "protocol not available") == false {
			fmt.Printf("ERR[%+v]", err)
		}
		return
	}
	//fmt.Printf("[%s]", ip.String())
	conn.Close()
	if err := insertDB(ip); err != nil {
		fmt.Printf("[err::%s]", err.Error())
	}
}
func insertDB(ip net.IP) error {
	Mu.Lock()
	defer Mu.Unlock()
	if _, err := DB.Exec("INSERT INTO host(ip) VALUES($1)", ip.String()); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil
		}
		return err
	}
	fmt.Printf("[%s]", ip.String())
	return nil
}

// to avoid being to hard on networks, we're traversing to the public internet like this:
// 0.0.0.0
// 1.0.0.0
// 2.0.0.0
// ...
// 255.0.0.0
// 0.1.0.0
// 1.1.0.0
// 2.1.0.0
// ...
func iterateThroughPublicIPs(queue chan net.IP) {
	ipstr := strings.Split(CURRENT_IP.String(), ".")
	ip := []int{0, 0, 0, 0}
	for i := 0; i < len(ipstr) && i < 4; i++ {
		if number, err := strconv.Atoi(ipstr[i]); err == nil {
			ip[i] = number
		}
	}

	for a0 := ip[3]; a0 <= 255; a0++ {
		ip[3] = 0
		for a1 := ip[2]; a1 <= 255; a1++ {
			ip[2] = 0
			fmt.Printf("\n+>x.x.%d.%d ", a1, a0)
			for a2 := ip[1]; a2 <= 255; a2++ {
				ip[1] = 0
				for a3 := ip[0]; a3 <= 255; a3++ {
					ip[0] = 0
					queue <- net.ParseIP(fmt.Sprintf("%d.%d.%d.%d", a3, a2, a1, a0))
				}
			}
		}
	}
}
