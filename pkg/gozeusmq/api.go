package gozeusmq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Offset int

const (
	FromStart   Offset = 0
	FromCurrent Offset = -1
)

type OffsetStrategy struct {
	Chapter int
	Offset  Offset
}

type ConfigC struct {
	Host     string
	ID       string
	Group    string
	Offset   OffsetStrategy
}

type zeusMqConsumer struct {
	c net.Conn
	s *bufio.Scanner
	toClose bool
}

type ZeusRequest struct {
	Topic   string
	Offset  uint64
	Message string
}

func (z *zeusMqConsumer) Close() error {
	z.toClose = true
	return z.c.Close()
}

func (z zeusMqConsumer) Handle(f func(r ZeusRequest) error) error {
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
		i, _ := strconv.Atoi(lineSpl[0])
		k := ZeusRequest{
			Offset:  uint64(i),
			Topic:   lineSpl[1],
			Message: lineSpl[2],
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

func(z zeusMqConsumer) readLine() (string, error) {
	if z.s.Scan(){
		return z.s.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}

func NewConsumer(c ConfigC) (zeusMqConsumer, error) {
	conn, err := net.Dial("tcp", c.Host)
	if err != nil {
		return zeusMqConsumer{}, err
	}
	_, err = conn.Write([]byte(fmt.Sprintf("c;%s;%s;%d\n",c.ID, c.Group, c.Offset)))
	if err != nil {
		return zeusMqConsumer{}, err
	}
	scanner := newScanner(conn)
	if scanner == nil {
		return zeusMqConsumer{}, err
	}
	return zeusMqConsumer{c: conn, s: scanner}, nil
}

type ConfigP struct {
	Host string
}

type zeusMqProducer struct {
	c net.Conn
	s *bufio.Scanner
}

func (z zeusMqProducer) Produce(topic string, msg string) (string, error) {
	msgF := fmt.Sprintf("%s;a;%s\n", topic, msg)
	_, err := z.c.Write([]byte(msgF))
	if err != nil {
		return "", err
	}
	line, err := z.readLine()
	if err != nil {
		return "", err
	}
	if line != "ok" {
		return "", fmt.Errorf("returned error")
	}
	return err
}

type SyncMessage struct {
	GoupMap map[string]SyncMessageG
}

type SyncMessageG struct {
	Read    int
	NotRead int
	Reboot  int
}

type ConfigS struct {
	Host  string
	MsgId string
}

type zeusMqSync struct {
	c net.Conn
	s *bufio.Scanner
}

func (z zeusMqSync) Sync(f func(SyncMessage)bool){
	if z.s.Scan(){

	}
}

func NewSync(c ConfigS) (*zeusMqSync, error) {
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
	return &zeusMqSync{
		c: conn,
		s: scanner,
	}, err
}

func(z zeusMqProducer) readLine() (string, error) {
	if z.s.Scan() {
		return z.s.Text(), nil
	}
	return "", fmt.Errorf("connection closed")
}

func NewProducer(c ConfigP) (zeusMqProducer, error) {
	conn, err := net.Dial("tcp", c.Host)
	if err != nil {
		return zeusMqProducer{}, err
	}
	_, err = conn.Write([]byte("p;;;;\n"))
	if err != nil {
		return zeusMqProducer{}, err
	}

	scanner := newScanner(conn)
	if scanner == nil {
		return zeusMqProducer{}, err
	}

	return zeusMqProducer{c: conn, s: scanner}, nil
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
