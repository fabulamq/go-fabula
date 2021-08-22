package gofabula

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/fabulamq/core/pkg/cnet"
	"net"
	"strconv"
	"strings"
)

type Offset int

type ConfigReader struct {
	Hosts  []string
	ID     string
	Mark   Mark
}

type Mark struct {
	Chapter int64
	Line    int64
}

type fabulaReader struct {
	c       net.Conn
	s       *bufio.Scanner
	toClose bool
}

type FabulaTail struct {
	Topic   string
	Chapter uint64
	Review  bool
	Line    uint64
	Message string
}

func (z *fabulaReader) Close() error {
	z.toClose = true
	return z.c.Close()
}

func (z fabulaReader) Read(f func(r FabulaTail) error) error {
	for {
		line, err := z.readLine()
		if z.toClose {
			return nil
		}
		if err != nil {
			z.c.Write([]byte("nok\n"))
			return err
		}
		lineSpl := strings.Split(line, ";")
		if lineSpl[0] != "msg"{
			continue
		}
		currChapter, _ := strconv.Atoi(lineSpl[1])
		currLine, _ := strconv.Atoi(lineSpl[2])
		review, _ := strconv.ParseBool(lineSpl[3])
		k := FabulaTail{
			Chapter: uint64(currChapter),
			Line:    uint64(currLine),
			Review:  review,
			Topic:   lineSpl[4],
			Message: lineSpl[5],
		}
		json.Unmarshal([]byte(line), &k)
		err = f(k)
		if err != nil {
			z.c.Write([]byte("nok\n"))
			return err
		}
		_, err = z.c.Write([]byte("ok\n"))
		if err != nil {
			return err
		}
	}
}

func (z fabulaReader) readLine() (string, error) {
	if z.s.Scan() {
		return z.s.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}

func NewStoryReader(c ConfigReader) (fabulaReader, error) {
	var err error
	var conn net.Conn
	for _, host := range c.Hosts{
		conn, err = cnet.Dial(host)
		if err != nil {
			return fabulaReader{}, err
		}
		_, err = conn.Write([]byte(fmt.Sprintf("sr;%s;%d;%d\n", c.ID, c.Mark.Chapter, c.Mark.Line)))
		if err != nil {
			return fabulaReader{}, err
		}
		scanner := newScanner(conn)
		if scanner == nil {
			return fabulaReader{}, err
		}
		return fabulaReader{c: conn, s: scanner}, nil
	}
	return fabulaReader{}, err
}

type ConfigWriter struct {
	Hosts []string
}

type fabulaStoryWriter struct {
	c net.Conn
	s *bufio.Scanner
}

func (z fabulaStoryWriter) Write(topic string, msg string) (*Mark, error) {
	msgF := fmt.Sprintf("%s;%s\n", topic, msg)
	_, err := z.c.Write([]byte(msgF))
	if err != nil {
		return nil, err
	}
	response, err := z.readLine()
	if err != nil {
		return nil, err
	}
	lineSpl := strings.Split(response, ";")
	if lineSpl[0] != "ok" {
		return nil, fmt.Errorf("returned error")
	}
	chapter, _ := strconv.ParseInt(lineSpl[1], 10, 64)
	line, _ := strconv.ParseInt(lineSpl[2], 10, 64)
	return &Mark{Chapter: chapter, Line: line}, err
}

type SyncMessage struct {
	ConsumerID string
	Status     ConsumerStatus
}

type ConsumerStatus string

const (
	CloseToRead ConsumerStatus = "almost"
	FarAway     ConsumerStatus = "faraway"
	ReadIt      ConsumerStatus = "readIt"
)

type ConfigS struct {
	Host string
	Mark *Mark
}

func (z fabulaStoryWriter) readLine() (string, error) {
	if z.s.Scan() {
		return z.s.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}

func NewStoryWriter(c ConfigWriter) (fabulaStoryWriter, error) {
	var err error
	var conn net.Conn
	for _, host := range c.Hosts {
		conn, err = cnet.Dial(host)
		if err != nil {
			return fabulaStoryWriter{}, err
		}
		_, err = conn.Write([]byte("sw;;;;\n"))
		if err != nil {
			return fabulaStoryWriter{}, err
		}

		scanner := newScanner(conn)
		if scanner == nil {
			return fabulaStoryWriter{}, fmt.Errorf("no scanner found")
		}

		return fabulaStoryWriter{c: conn, s: scanner}, nil
	}
	return fabulaStoryWriter{}, err
}

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
