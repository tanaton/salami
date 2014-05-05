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
	rch chan<- bool
}

type ipPacket struct {
	host string
	rch  chan<- string
}

type Config struct {
	Addr             string
	Port             int
	ReadTimeoutSec   int
	WriteTimeoutSec  int
	ServerTimeoutSec int
	ProxyTimeoutSec  int
	MaxHeaderBytes   int
	LogFilePath      string
	URLWhiteList     []string
	URLWhiteListReg  []*regexp.Regexp `json:"-"`
	BalanceList      []Balance
	Heartbeat        bool
	IPCacheTimeSec   int
}

type SummaryHandle struct {
	conf    *Config
	timeout time.Duration
	f       func(network, addr string) (net.Conn, error)
	lb      <-chan Balance
	sesCh   chan<- sesPacket
	ipCh    chan<- ipPacket
}

type heartbeatHandle struct {
	text string
}

const (
	LOAD_BALANCE_BUF       = 32
	CONFIG_JSON_PATH_DEF   = "salami.config.json"
	ADDR_DEF               = ""
	PORT_DEF               = 18080
	READ_TIMEOUT_SEC_DEF   = 15
	WRITE_TIMEOUT_SEC_DEF  = 15
	SERVER_TIMEOUT_SEC_DEF = 13
	PROXY_TIMEOUT_SEC_DEF  = 12
	LOG_FILE_PATH_DEF      = ""
	MAX_HEADER_BYTES_DEF   = 1024 * 10
)

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
		w = fp
	}
	g_log = log.New(w, "", log.Ldate|log.Ltime|log.Lmicroseconds)

	if c.Heartbeat {
		// ハートビートを有効化
		heartbeat := &http.Server{
			Addr:           ":80",
			Handler:        &heartbeatHandle{text: "やっはろー"},
			ReadTimeout:    time.Duration(c.ReadTimeoutSec) * time.Second,
			WriteTimeout:   time.Duration(c.WriteTimeoutSec) * time.Second,
			MaxHeaderBytes: c.MaxHeaderBytes,
		}
		go heartbeat.ListenAndServe()
	}

	t := time.Duration(c.ProxyTimeoutSec) * time.Second
	myHandler := &SummaryHandle{
		conf:    c,
		timeout: t,
		f:       createDialTimeout(t),
		lb:      loadBalancing(c.BalanceList),
		sesCh:   sesProc(),
		ipCh:    cacheIP(time.Duration(c.IPCacheTimeSec)*time.Second, t),
	}
	server := &http.Server{
		Addr:           fmt.Sprintf("%s:%d", c.Addr, c.Port),
		Handler:        http.TimeoutHandler(myHandler, time.Duration(c.ServerTimeoutSec)*time.Second, "timeout!!!"),
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
		m := make(map[string]struct{})
		for it := range reqch {
			if it.rch != nil {
				_, ok := m[it.key]
				it.rch <- ok
				m[it.key] = struct{}{}
			} else {
				delete(m, it.key)
			}
		}
	}(reqch)
	return reqch
}

func cacheIP(timeout, deadline time.Duration) chan<- ipPacket {
	if timeout <= 0 {
		return nil
	}
	reqc := make(chan ipPacket, 4)
	go func() {
		delc := time.Tick(timeout)
		m := make(map[string]string, 32)
		for {
			select {
			case <-delc:
				// 定期的に削除
				m = make(map[string]string, 32)
			case it := <-reqc:
				ip, ok := m[it.host]
				if !ok {
					iplist, err := lookupIPDeadline(it.host, deadline)
					if err == nil {
						m[it.host] = iplist[0].String()
					}
				}
				it.rch <- ip
			}
		}
	}()
	return reqc
}

func loadBalancing(bl []Balance) <-chan Balance {
	if bl == nil {
		return nil
	}
	ch := make(chan Balance, LOAD_BALANCE_BUF)
	go func(ch chan<- Balance) {
		max := len(bl)
		i := 0
		for {
			ch <- bl[i]
			i = (i + 1) % max
		}
	}(ch)
	return ch
}

func (sh *SummaryHandle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u := strings.TrimLeft(r.URL.Path, "/")
	pl := strings.Split(u, "/")
	if len(pl) < 2 {
		// 異常
		badRequest(w)
		g_log.Printf("Message:Path error\tPath:%s", u)
		return
	}
	// ホワイトリストの確認
	if sh.checkUrlWhiteList(u) == false {
		badRequest(w)
		g_log.Printf("Message:WhiteList error\tPath:%s", u)
		return
	}

	if sh.lockSession(u) {
		// 衝突
		conflictResponse(w)
		g_log.Printf("Message:Conflict\tPath:%s", u)
	} else {
		// 1度目のアクセス
		// バランスする
		var lbhost Balance
		if sh.lb != nil {
			lbhost = <-sh.lb
		} else {
			lbhost = Balance{
				Host: "",
				Port: 80,
			}
		}
		// この処理に時間がかかる
		code, err := sh.httpCopy(pl, lbhost, w, r)

		if err == nil {
			g_log.Printf("Host:%s\tPort:%d\tCode:%d\tPath:%s", lbhost.Host, lbhost.Port, code, u)
		} else {
			gatewayTimeoutResponse(w)
			g_log.Printf("Message:Bad Response\tHost:%s\tPort:%d\tError:%s\tPath:%s", lbhost.Host, lbhost.Port, u, err.Error())
		}

		// セッションの解除
		sh.unlockSession(u)
	}
}

func (sh *SummaryHandle) lockSession(u string) (ret bool) {
	ch := make(chan bool, 1)
	sh.sesCh <- sesPacket{
		key: u,
		rch: ch,
	}
	ret = <-ch
	close(ch)
	return
}

func (sh *SummaryHandle) unlockSession(u string) {
	sh.sesCh <- sesPacket{
		key: u,
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
	msg  string
}

func (e *RedirectError) Error() string {
	return e.msg
}

func redirectPolicy(r *http.Request, _ []*http.Request) (err error) {
	return &RedirectError{
		host: r.URL.Host,
		path: r.URL.Path,
		msg:  "redirect error",
	}
}

// TCP接続
func (sh *SummaryHandle) httpCopy(s []string, lbhost Balance, w http.ResponseWriter, r *http.Request) (int, error) {
	if lbhost.Host == "" {
		lbhost.Host = s[0]
		s = s[1:]
	}
	req, err := http.NewRequest(r.Method, fmt.Sprintf("http://%s:%d/%s", lbhost.Host, lbhost.Port, strings.Join(s, "/")), nil)
	if err != nil {
		return 0, err
	}
	if sh.ipCh != nil && net.ParseIP(lbhost.Host) == nil {
		// IP cache有効、かつIPではなくホスト名でリクエストを受けた場合
		ch := make(chan string, 1)
		sh.ipCh <- ipPacket{lbhost.Host, ch}
		ip := <-ch
		close(ch)
		req.Host = req.URL.Host
		req.URL.Host = fmt.Sprintf("%s:%d", ip, lbhost.Port)
	}
	for key, _ := range r.Header {
		req.Header.Set(key, r.Header.Get(key))
	}
	req.Header.Set("Connection", "close")
	client := &http.Client{
		Transport: &http.Transport{
			Dial:                  sh.f,
			DisableKeepAlives:     true,
			DisableCompression:    true, // 圧縮解凍は全てこっちで指示する
			ResponseHeaderTimeout: sh.timeout,
		},
		CheckRedirect: redirectPolicy,
	}
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
	// ヘッダーを書き込む
	for key, _ := range resp.Header {
		w.Header().Set(key, resp.Header.Get(key))
	}
	w.Header().Set("Connection", "close")
	w.WriteHeader(resp.StatusCode)
	// 本文を書き込む
	io.Copy(w, resp.Body)
	// クローズする
	resp.Body.Close()
	return resp.StatusCode, nil
}

func (hbh *heartbeatHandle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// とりあえず全部正常
	w.Write([]byte(hbh.text))
	g_log.Println("Message:Health Check OK")
}

// src/pkg/net/lookup.goを参考に作成
func lookupIPDeadline(host string, deadline time.Duration) (addrs []net.IP, err error) {
	if deadline <= 0 {
		return net.LookupIP(host)
	}
	t := time.NewTimer(deadline)
	defer t.Stop()
	type res struct {
		addrs []net.IP
		err   error
	}
	resc := make(chan res, 1)
	go func() {
		a, err := net.LookupIP(host)
		resc <- res{a, err}
	}()
	select {
	case <-t.C:
		err = errors.New("ホストの名前解決がタイムアウトしました")
	case r := <-resc:
		addrs, err = r.addrs, r.err
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
	c.readDefault()
	err := c.readConfig(path)
	if err != nil {
		panic(err)
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

	l := len(c.URLWhiteList)
	if l > 0 {
		c.URLWhiteListReg = make([]*regexp.Regexp, l)
		for i, it := range c.URLWhiteList {
			c.URLWhiteListReg[i] = regexp.MustCompile(it)
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
	c.ServerTimeoutSec = SERVER_TIMEOUT_SEC_DEF
	c.ProxyTimeoutSec = PROXY_TIMEOUT_SEC_DEF
	c.MaxHeaderBytes = MAX_HEADER_BYTES_DEF
	c.LogFilePath = LOG_FILE_PATH_DEF
	c.URLWhiteListReg = nil
	c.BalanceList = nil
	c.Heartbeat = false
	c.IPCacheTimeSec = 0
}
