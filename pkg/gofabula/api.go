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

type ConfigC struct {
	Host     string
	ID       string
	Mark     Mark
}

type Mark struct {
	Chapter int64
	Line    int64
}

type zeusMqConsumer struct {
	c net.Conn
	s *bufio.Scanner
	toClose bool
}

type ZeusRequest struct {
	Topic   string
	Chapter uint64
	Line    uint64
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
		currChapter, _ := strconv.Atoi(lineSpl[0])
		currLine, _ := strconv.Atoi(lineSpl[1])

		k := ZeusRequest{
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
	_, err = conn.Write([]byte(fmt.Sprintf("c;%s;%d;%d\n",c.ID, c.Mark.Chapter, c.Mark.Line)))
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
	msgF := fmt.Sprintf("%s;%s\n", topic, msg)
	_, err := z.c.Write([]byte(msgF))
	if err != nil {
		return "", err
	}
	line, err := z.readLine()
	if err != nil {
		return "", err
	}
	lineSpl := strings.Split(line, ";")
	if lineSpl[0] != "ok" {
		return "", fmt.Errorf("returned error")
	}
	return lineSpl[1], err
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
