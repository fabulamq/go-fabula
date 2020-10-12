package gofabula

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Offset int

type ConfigReader struct {
	Host     string
	ID       string
	Mark     Mark
}

type Mark struct {
	Chapter int64
	Line    int64
}

type fabulaReader struct {
	c net.Conn
	s *bufio.Scanner
	toClose bool
}

type FabulaLine struct {
	Topic   string
	Chapter uint64
	Line    uint64
	Message string
}

func (z *fabulaReader) Close() error {
	z.toClose = true
	return z.c.Close()
}

func (z fabulaReader) Read(f func(r FabulaLine) error) error {
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
		currChapter, _ := strconv.Atoi(lineSpl[0])
		currLine, _ := strconv.Atoi(lineSpl[1])

		k := FabulaLine{
			Chapter: uint64(currChapter),
			Line:    uint64(currLine),
			Topic:   lineSpl[2],
			Message: lineSpl[3],
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

func(z fabulaReader) readLine() (string, error) {
	if z.s.Scan(){
		return z.s.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}

func NewStoryReader(c ConfigReader) (fabulaReader, error) {
	conn, err := net.Dial("tcp", c.Host)
	if err != nil {
		return fabulaReader{}, err
	}
	_, err = conn.Write([]byte(fmt.Sprintf("sr;%s;%d;%d\n",c.ID, c.Mark.Chapter, c.Mark.Line)))
	if err != nil {
		return fabulaReader{}, err
	}
	scanner := newScanner(conn)
	if scanner == nil {
		return fabulaReader{}, err
	}
	return fabulaReader{c: conn, s: scanner}, nil
}

type ConfigWriter struct {
	Host string
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
	chapter,_ := strconv.ParseInt(lineSpl[1], 10, 64)
	line,_ := strconv.ParseInt(lineSpl[2], 10, 64)
	return &Mark{Chapter: chapter, Line: line}, err
}

type SyncMessage struct {
	ConsumerID string
	Status     ConsumerStatus

}

type ConsumerStatus string

const  (
	CloseToRead ConsumerStatus = "almost"
	FarAway     ConsumerStatus = "faraway"
	ReadIt      ConsumerStatus = "readIt"
)

type ConfigS struct {
	Host string
	Mark *Mark
}

type fabulaSync struct {
	c net.Conn
	s *bufio.Scanner
}

func (z fabulaSync) Sync(f func(SyncMessage)bool){
	if z.s.Scan(){

	}
}

func NewSync(c ConfigS) (*fabulaSync, error) {
	conn, err := net.Dial("tcp", c.Host)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte("s;\n"))
	if err != nil {
		return nil, err
	}
	scanner := newScanner(conn)
	if scanner == nil {
		return nil, err
	}
	return &fabulaSync{
		c: conn,
		s: scanner,
	}, err
}

func(z fabulaStoryWriter) readLine() (string, error) {
	if z.s.Scan() {
		return z.s.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}

func NewStoryWriter(c ConfigWriter) (fabulaStoryWriter, error) {
	conn, err := net.Dial("tcp", c.Host)
	if err != nil {
		return fabulaStoryWriter{}, err
	}
	_, err = conn.Write([]byte("sw;;;;\n"))
	if err != nil {
		return fabulaStoryWriter{}, err
	}

	scanner := newScanner(conn)
	if scanner == nil {
		return fabulaStoryWriter{}, err
	}

	return fabulaStoryWriter{c: conn, s: scanner}, nil
}

func newScanner(c net.Conn)*bufio.Scanner{
	scanner := bufio.NewScanner(c)
	scanner.Split(bufio.ScanLines)
	if scanner.Scan() {
		if scanner.Text() == "ok"{
			return scanner
		}
	}
	return nil
}
