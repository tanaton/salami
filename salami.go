package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

type Balance struct {
	Host string
	Port int
}

type sesPacket struct {
	key string
	del bool
	rch chan<- bool
}

type Config struct {
	Addr            string
	Port            int
	ReadTimeoutSec  int
	WriteTimeoutSec int
	ProxyTimeoutSec int
	MaxHeaderBytes  int
	LogFilePath     string
	URLWhiteList    []string
	URLWhiteListReg []*regexp.Regexp `json:"-"`
	BalanceList     []Balance
}

type SummaryHandle struct {
	conf    *Config
	timeout time.Duration
	f       func(network, addr string) (net.Conn, error)
	lb      <-chan Balance
	sesCh   chan<- sesPacket
}

const (
	CRLF_STR             = "\r\n"
	LOAD_BALANCE_BUF     = 32
	CONFIG_JSON_PATH_DEF = "salami.config.json"

	ADDR_DEF              = ""
	PORT_DEF              = 18080
	READ_TIMEOUT_SEC_DEF  = 15
	WRITE_TIMEOUT_SEC_DEF = 15
	PROXY_TIMEOUT_SEC_DEF = 12
	LOG_FILE_PATH_DEF     = ""
	MAX_HEADER_BYTES_DEF  = 1024 * 10
)

var g_balance_def = []Balance{
	Balance{
		Host: "",
		Port: 80,
	},
}
var g_log *log.Logger

func main() {
	c := readConfig()
	var w io.Writer
	if c.LogFilePath == "" {
		w = os.Stdout
	} else {
		fp, err := os.OpenFile(c.LogFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal("file open error")
		}
		//defer fp.Close()	実行終了で開放
		w = fp
	}
	g_log = log.New(w, "", log.Ldate|log.Ltime|log.Lmicroseconds)

	t := time.Duration(c.ProxyTimeoutSec) * time.Second
	myHandler := &SummaryHandle{
		conf:    c,
		timeout: t,
		f:       createDialTimeout(t),
		lb:      loadBalancing(c.BalanceList),
		sesCh:   sesProc(),
	}
	server := &http.Server{
		Addr:           fmt.Sprintf("%s:%d", c.Addr, c.Port),
		Handler:        myHandler,
		ReadTimeout:    time.Duration(c.ReadTimeoutSec) * time.Second,
		WriteTimeout:   time.Duration(c.WriteTimeoutSec) * time.Second,
		MaxHeaderBytes: c.MaxHeaderBytes,
	}
	// サーバ起動
	g_log.Fatal(server.ListenAndServe())
}

func sesProc() chan<- sesPacket {
	reqch := make(chan sesPacket, 4)
	go func(reqch <-chan sesPacket) {
		m := make(map[string]bool)
		for {
			select {
			case it := <-reqch:
				if it.rch != nil {
					_, ok := m[it.key]
					it.rch <- ok
				} else if it.del {
					delete(m, it.key)
				} else {
					m[it.key] = true
				}
			}
		}
	}(reqch)
	return reqch
}

func loadBalancing(bl []Balance) <-chan Balance {
	ch := make(chan Balance, LOAD_BALANCE_BUF)
	go func() {
		max := len(bl)
		i := 0
		for {
			ch <- bl[i]
			i = (i + 1) % max
		}
	}()
	return ch
}

func (sh *SummaryHandle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pl, err := createPathList(r.URL)
	if err != nil {
		// 異常
		badRequest(w)
		g_log.Printf("Path error :%s", r.URL)
		return
	}
	u := "http://" + strings.Join(pl, "/")

	// ホワイトリストの確認
	if sh.checkUrlWhiteList(u) == false {
		badRequest(w)
		g_log.Printf("WhiteList error :%s", u)
		return
	}

	if ok := sh.getSesMap(u); ok {
		// 衝突
		conflictResponse(w)
		g_log.Printf("Conflict %s", u)
	} else {
		// 1度目のアクセス
		sh.setSesMap(u)
		defer sh.delSesMap(u)

		lbhost := <-sh.lb
		sl := updatePathList(lbhost.Host, pl)
		// この処理に時間がかかる
		code, err := httpCopy(sl, lbhost.Port, w, r, &http.Client{
			Transport: &http.Transport{
				Dial:                  sh.f,
				DisableKeepAlives:     true,
				DisableCompression:    true, // 圧縮解凍は全てこっちで指示する
				ResponseHeaderTimeout: sh.timeout,
			},
			CheckRedirect: redirectPolicy,
		})

		if err == nil {
			g_log.Printf("%d %s", code, u)
		} else {
			gatewayTimeoutResponse(w)
			g_log.Printf("Bad Response => Host:%s Port:%d URL:%s", lbhost.Host, lbhost.Port, u)
		}
	}
}

func (sh *SummaryHandle) getSesMap(u string) bool {
	ch := make(chan bool, 1)
	defer close(ch)
	sh.sesCh <- sesPacket{
		key: u,
		rch: ch,
	}
	return <-ch
}

func (sh *SummaryHandle) setSesMap(u string) {
	sh.sesCh <- sesPacket{
		key: u,
	}
}

func (sh *SummaryHandle) delSesMap(u string) {
	sh.sesCh <- sesPacket{
		key: u,
		del: true,
	}
}

func (sh *SummaryHandle) checkUrlWhiteList(u string) bool {
	if sh.conf.URLWhiteListReg == nil {
		// ホワイトリストが無い
		return true
	}
	// 正規表現は複数のゴールーチンから扱える
	for _, reg := range sh.conf.URLWhiteListReg {
		if reg.MatchString(u) {
			// リストと一致
			return true
		}
	}
	return false
}

func badRequest(w http.ResponseWriter) {
	// ヘッダーを書き込む
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusBadRequest) // 400
}

func conflictResponse(w http.ResponseWriter) {
	// ヘッダーを書き込む
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusConflict) // 409
}

func gatewayTimeoutResponse(w http.ResponseWriter) {
	// ヘッダーを書き込む
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusGatewayTimeout) // 504
}

func createDialTimeout(t time.Duration) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		con, err := net.DialTimeout(network, addr, t)
		if err == nil {
			con.SetDeadline(time.Now().Add(t))
		}
		return con, err
	}
}

type RedirectError struct {
	host string
	path string
	msg string
}
func (e *RedirectError) Error() string {
	return e.msg
}

func redirectPolicy(r *http.Request, _ []*http.Request) (err error) {
	return &RedirectError{
		host: r.URL.Host,
		path: r.URL.Path,
		msg: "redirect error",
	}
}

// TCP接続
func httpCopy(s []string, port int, w http.ResponseWriter, r *http.Request, client *http.Client) (int, error) {
	req, err := http.NewRequest(r.Method, fmt.Sprintf("http://%s:%d/%s", s[0], port, strings.Join(s[1:], "/")), nil)
	if err != nil {
		return 0, err
	}
	for key, _ := range r.Header {
		req.Header.Set(key, r.Header.Get(key))
	}
	req.Header.Set("Connection", "close")
	resp, err := client.Do(req)
	if err != nil {
		if resp == nil {
			return 0, err
		}
		uerr, ok := err.(*url.Error)
		if !ok {
			return 0, err
		}
		_, rok := uerr.Err.(*RedirectError)
		if !rok {
			return 0, err
		}
		// RedirectErrorならば処理継続
	}
	defer resp.Body.Close()

	// ヘッダーを書き込む
	for key, _ := range resp.Header {
		w.Header().Set(key, resp.Header.Get(key))
	}
	w.Header().Set("Connection", "close")
	w.WriteHeader(resp.StatusCode)
	// 本文を書き込む
	io.Copy(w, resp.Body)
	return resp.StatusCode, nil
}

func createPathList(u *url.URL) (pl []string, err error) {
	pl = strings.Split(strings.TrimLeft(u.Path, "/"), "/")
	if len(pl) < 2 {
		err = errors.New("Invalid request")
	}
	return
}

func updatePathList(host string, pl []string) (sl []string) {
	if host != "" {
		sl = append([]string{host}, pl...)
	} else {
		sl = pl
	}
	return
}

func readConfig() *Config {
	c := &Config{}
	argc := len(os.Args)
	var path string
	if argc == 2 {
		path = os.Args[1]
	} else {
		path = CONFIG_JSON_PATH_DEF
	}
	err := c.readConfig(path)
	if err != nil {
		c.readDefault()
	}
	return c
}

func (c *Config) readConfig(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, c)
	if err != nil {
		return err
	}

	if len(c.BalanceList) == 0 {
		c.BalanceList = g_balance_def
	}
	if len(c.URLWhiteList) > 0 {
		c.URLWhiteListReg = []*regexp.Regexp{}
		for _, it := range c.URLWhiteList {
			c.URLWhiteListReg = append(c.URLWhiteListReg, regexp.MustCompile(it))
		}
	} else {
		c.URLWhiteListReg = nil
	}
	return nil
}

func (c *Config) readDefault() {
	c.Addr = ADDR_DEF
	c.Port = PORT_DEF
	c.ReadTimeoutSec = READ_TIMEOUT_SEC_DEF
	c.WriteTimeoutSec = WRITE_TIMEOUT_SEC_DEF
	c.ProxyTimeoutSec = PROXY_TIMEOUT_SEC_DEF
	c.MaxHeaderBytes = MAX_HEADER_BYTES_DEF
	c.LogFilePath = LOG_FILE_PATH_DEF
	c.URLWhiteListReg = nil
	c.BalanceList = g_balance_def
}
