package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"net"
	"sync"
)

var (
	DB          *sql.DB = nil
	CONCURRENCY int     = 500
	Mu          sync.Mutex
)

func setup() (err error) {
	DB, err = sql.Open("sqlite3", "../ftp.sqlite")
	if err != nil {
		return err
	}
	if _, err = DB.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return err
	} else if _, err = DB.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return err
	}
	if _, err = DB.Exec(`CREATE TABLE IF NOT EXISTS dns (
  related_ip TEXT,
  domain TEXT,
  FOREIGN KEY(related_ip) REFERENCES host(ip)

)`); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := setup(); err != nil {
		fmt.Printf("ERR %+v", err)
		return
	}

	queue := make(chan string, 2000)

	stmt, err := DB.Prepare("INSERT INTO dns(related_ip, domain) VALUES($1, $2)")
	if err != nil {
		fmt.Printf("ERROR %s\n", err.Error())
		return
	}
	for i := 0; i < CONCURRENCY; i++ {
		go runner(stmt, queue)
	}

	rows, err := DB.Query("SELECT host.ip FROM host LEFT JOIN dns ON host.ip = dns.related_ip WHERE dns.domain IS NULL")
	if err != nil {
		fmt.Printf("ERR %+v", err)
		return
	}
	for rows.Next() {
		ip := ""
		rows.Scan(&ip)
		queue <- ip
	}
	close(queue)
}

func runner(stmt *sql.Stmt, queue chan string) {
	for ip := range queue {
		s, _ := net.LookupAddr(ip)
		if len(s) == 0 {
			insertDB(stmt, ip, "")
		}
		for i := 0; i < len(s); i++ {
			insertDB(stmt, ip, s[i])
		}
	}
}

func insertDB(stmt *sql.Stmt, ip string, domain string) {
	Mu.Lock()
	defer Mu.Unlock()
	fmt.Printf("> insert %+v | %+v\n", ip, domain)
	if _, err := stmt.Exec(ip, domain); err != nil {
		fmt.Printf("ERR %+v", err)
	}
}
