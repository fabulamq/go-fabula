package gofabula

import (
	"bufio"
	"net"
)

func newScanner(c net.Conn) *bufio.Scanner {
	scanner := bufio.NewScanner(c)
	scanner.Split(bufio.ScanLines)
	if scanner.Scan() {
		if scanner.Text() == "ok" {
			return scanner
		}
	}
	return nil
}
