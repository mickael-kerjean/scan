package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	DB          *sql.DB = nil
	CONCURRENCY int     = 25000
	Mu          sync.Mutex
)

func main() {
	if err := setup(); err != nil {
		fmt.Printf("ERR %+v", err)
		return
	}
	queue := make(chan net.IP, 25000)

	stmt, err := DB.Prepare("INSERT INTO details(related_ip, available, ftps, anonymous, stream) VALUES($1, $2, $3, $4, $5)")
	if err != nil {
		fmt.Printf("ERR %s\n", err.Error())
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < CONCURRENCY; i++ {
		wg.Add(1)
		go func() {
			runner(stmt, queue)
			wg.Done()
		}()
	}
	rows, err := DB.Query("SELECT host.ip FROM host LEFT JOIN details ON host.ip = details.related_ip WHERE details.available IS NULL")
	if err != nil {
		fmt.Printf("ERR %+v", err)
		return
	}
	for rows.Next() {
		ip := ""
		rows.Scan(&ip)
		queue <- net.ParseIP(ip)
	}
	wg.Wait()
	close(queue)
	DB.Close()
}

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
	if _, err = DB.Exec(`CREATE TABLE IF NOT EXISTS details (
	  related_ip TEXT,
	  available BOOL,
      anonymous BOOL,
	  ftps BOOL,
      stream TEXT,
	  FOREIGN KEY(related_ip) REFERENCES host(ip)
	)`); err != nil {
		return err
	}
	return nil
}

func runner(stmt *sql.Stmt, queue chan net.IP) {
	for ip := range queue {
		//fmt.Printf("%+v\n", ip)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:21", ip.String()), 1*time.Second)
		if err != nil {
			fmt.Printf("%v => %+v\n", ip, err)
			insertDB(stmt, false, ip.String(), false, false, err.Error())
			continue
		}
		defer conn.Close()
		fmt.Fprintf(conn, "USER anonymous\r\n")
		fmt.Fprintf(conn, "PASS anonymous\r\n")
		fmt.Fprintf(conn, "SYST anonymous\r\n")
		fmt.Fprintf(conn, "FEAT anonymous\r\n")
		fmt.Fprintf(conn, "QUIT\r\n")
		result := struct {
			Content   string
			Ftps      bool
			Anonymous bool
		}{"", false, false}

		s := bufio.NewScanner(conn)
		msg := make(chan string)
		go func() {
			for s.Scan() {
				line := s.Text()
				if strings.HasPrefix(line, "AUTH TLS") {
					result.Ftps = true
				} else if strings.HasPrefix(line, "230 ") {
					result.Anonymous = true
				}
				result.Content += line + "\n"
			}
			msg <- "OK"
		}()
		select {
		case <-time.After(time.Second * 1):
			insertDB(stmt, false, ip.String(), false, false, "")
		case <-msg:
			insertDB(stmt, true, ip.String(), result.Ftps, result.Anonymous, result.Content)
		}
	}
}

func insertDB(stmt *sql.Stmt, available bool, ip string, ftps bool, anonymous bool, content string) {
	Mu.Lock()
	if _, err := stmt.Exec(ip, available, ftps, anonymous, content); err != nil {
		fmt.Printf("ERR %+v", err)
	}
	Mu.Unlock()
}
