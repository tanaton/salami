package main

import (
	"bufio"
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
	"strconv"
	"strings"
	"sync"
	"time"
)

type Balance struct {
	Host	string
	Port	int
}

type Config struct {
	v					map[string]interface{}
	Addr				string
	Port				int
	ReadTimeoutSec		int
	WriteTimeoutSec		int
	MaxHeaderBytes		int
	LogFilePath			string
	URLWhiteList		[]*regexp.Regexp
	BalanceList			[]*Balance
}

type Session struct {
	url		string
	mproc	sync.RWMutex
	wch		chan http.ResponseWriter
}

type SammaryHandle struct {
	conf	*Config
	logger	*log.Logger
	lb		<-chan Balance
	sesMux	sync.RWMutex
	sesMap	map[string]*Session
}

const (
	TIMEOUT_NSEC	time.Duration	= 8 * 1000 * 1000 * 1000
	CRLF_STR						= "\r\n"
	INTVAL_CONF_ERR					= 0x80000000
	LOAD_BALANCE_BUF				= 32
	WRITE_CHAN_BUF					= 4
	CONFIG_JSON_PATH_DEF			= "salami.config.json"
	ERROR_STATUS_CODE_DEF			= 400

	ADDR_DEF						= ""
	PORT_DEF						= 18080
	READ_TIMEOUT_SEC_DEF			= 10
	WRITE_TIMEOUT_SEC_DEF			= 10
	LOG_FILE_PATH_DEF				= ""
	MAX_HEADER_BYTES_DEF			= 1024 * 10
)

var g_balance_def = []*Balance{
	&Balance{
		Host	: "",
		Port	: 80,
	},
}

func main() {
	c := readConfig()
	var w io.Writer
	if c.LogFilePath == "" {
		w = os.Stdout
	} else {
		fp, err := os.OpenFile(c.LogFilePath, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
		if err != nil { log.Fatal("file open error") }
		defer fp.Close()
		w = fp
	}
	logger := log.New(w, "", log.Ldate | log.Ltime | log.Lmicroseconds)

	myHandler := &SammaryHandle{
		conf	: c,
		logger	: logger,
		lb		: loadBalancing(c.BalanceList),
		sesMap	: make(map[string]*Session),
	}
	server := &http.Server{
		Addr			: fmt.Sprintf("%s:%d", c.Addr, c.Port),
		Handler			: myHandler,
		ReadTimeout		: time.Duration(c.ReadTimeoutSec) * time.Second,
		WriteTimeout	: time.Duration(c.WriteTimeoutSec) * time.Second,
		MaxHeaderBytes	: c.MaxHeaderBytes,
	}
	// サーバ起動
	logger.Fatal(server.ListenAndServe())
}

func loadBalancing(bl []*Balance) <-chan Balance {
	ch := make(chan Balance, LOAD_BALANCE_BUF)
	go func() {
		max := len(bl)
		i := 0
		for {
			ch <- *(bl[i])
			i = (i + 1) % max
		}
	}()
	return ch
}

func (sh *SammaryHandle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pl, err := createPathList(r.URL)
	if err != nil {
		// 異常
		w.WriteHeader(ERROR_STATUS_CODE_DEF)
		sh.logger.Print("Path error")
		return
	}
	u := "http://" + strings.Join(pl, "/")

	// ホワイトリストの確認
	if sh.conf.URLWhiteList != nil && !sh.checkUrlWhiteList(u) {
		w.WriteHeader(ERROR_STATUS_CODE_DEF)
		sh.logger.Print("WhiteList error")
		return
	}

	if ses, ok := sh.getSesMap(u); ok {
		ses.wch <- w

		// 続く処理を止める
		ses.mproc.Lock()
		defer ses.mproc.Unlock()

		sh.logger.Printf("Collision %s", u)
	} else {
		// 1度目のアクセス
		se := &Session{
			url		: u,
			wch		: make(chan http.ResponseWriter, WRITE_CHAN_BUF),
		}
		// 続く処理を止める
		se.mproc.Lock()
		defer se.mproc.Unlock()

		// 設定
		se.wch <- w

		sh.setSesMap(u, se)
		lbhost := <-sh.lb
		sl := updatePathList(lbhost.Host, pl)
		// この処理に時間がかかる
		data, res, err := httpDownload(sl, lbhost.Port, r, TIMEOUT_NSEC)
		sh.delSesMap(u)

		if err == nil {
			se.transfer(data, res)
			sh.logger.Printf("%d %s", res.StatusCode, u)
		} else {
			se.transferBad()
			sh.logger.Print("Bad request!")
		}
	}
}

func (sh *SammaryHandle) getSesMap(u string) (ses *Session, ok bool) {
	sh.sesMux.Lock()
	defer sh.sesMux.Unlock()

	ses, ok = sh.sesMap[u];
	return
}

func (sh *SammaryHandle) setSesMap(u string, ses *Session) {
	sh.sesMux.Lock()
	defer sh.sesMux.Unlock()

	sh.sesMap[u] = ses
}

func (sh *SammaryHandle) delSesMap(u string) {
	sh.sesMux.Lock()
	defer sh.sesMux.Unlock()

	delete(sh.sesMap, u)
}

func (sh *SammaryHandle) checkUrlWhiteList(u string) bool {
	for _, reg := range sh.conf.URLWhiteList {
		if reg.MatchString(u) {
			return true
		}
	}
	return false
}

func (se *Session) getWriteList() []http.ResponseWriter {
	wlist := make([]http.ResponseWriter, 0, WRITE_CHAN_BUF)
	lp := 0

CREATE_WRITE_LIST:
	for lp < 10 {
		select {
		case resw, ok := <-se.wch:
			if ok {
				wlist = append(wlist, resw)
			} else {
				break CREATE_WRITE_LIST
			}
		default:
			// 通信なし
			lp++
		}
	}
	close(se.wch)

	return wlist
}

func (se *Session) transfer(data []byte, res *http.Response) {
	wlist := se.getWriteList()
	para := len(wlist)
	sync := make(chan bool, para)
	defer close(sync)

	for _, resw := range wlist {
		// ネットワーク書き込みは並列で実行
		sync <- true
		go func(w http.ResponseWriter) {
			// ヘッダーを書き込む
			w.Header().Set("Connection", "close")
			for key, _ := range res.Header {
				w.Header().Set(key, res.Header.Get(key))
			}
			w.WriteHeader(res.StatusCode)
			// 本文を書き込む
			w.Write(data)
			<-sync
		}(resw)
	}
	for ; para > 0; para-- {
		sync <- true
	}
}

func (se *Session) transferBad() {
	wlist := se.getWriteList()
	para := len(wlist)
	sync := make(chan bool, para)
	defer close(sync)

	for _, resw := range wlist {
		// ネットワーク書き込みは並列で実行
		sync <- true
		go func(w http.ResponseWriter) {
			// ヘッダーを書き込む
			w.WriteHeader(ERROR_STATUS_CODE_DEF)
			<-sync
		}(resw)
	}
	for ; para > 0; para-- {
		sync <- true
	}
}

// TCP接続
func httpDownload(s []string, port int, r *http.Request, timeout time.Duration) (data []byte, res *http.Response, err error){
	// タイムアウトを設定(ms単位)
	if con, e := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", s[0], port), timeout); e == nil {
		// 接続を閉じる
		defer con.Close()
		var req *http.Request
		req, err = http.NewRequest("GET", "http://" + strings.Join(s, "/"), nil)
		if err != nil { return }
		// ヘッダー送信
		con.Write(createBaseHeader(s, r))
		r.Header.Write(con)
		con.Write([]byte(CRLF_STR))
		// ヘッダー受信
		res, err = http.ReadResponse(bufio.NewReader(con), req)
		if err != nil { return }
		defer res.Body.Close()
		data, err = ioutil.ReadAll(res.Body)
	} else {
		// エラーをセット
		err = e
	}
	return
}

func createBaseHeader(s []string, r *http.Request) []byte {
	proto := r.Method + " /" + strings.Join(s[1:], "/") + " " + r.Proto + CRLF_STR
	host := "Host: " + s[0] + CRLF_STR
	return []byte(proto + host)
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
		sl = make([]string, 0, 1)
		sl = append(sl, host)
		sl = append(sl, pl...)
	} else {
		sl = pl
	}
	return
}

func readConfig() *Config {
	c := &Config{v: make(map[string]interface{})}
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
	if err != nil { return err }
	err = json.Unmarshal(data, &c.v)
	if err != nil { return err }
	c.Addr = c.getDataString("Addr", ADDR_DEF)
	c.Port = c.getDataInt("Port", PORT_DEF)
	c.ReadTimeoutSec = c.getDataInt("ReadTimeoutSec", READ_TIMEOUT_SEC_DEF)
	c.WriteTimeoutSec = c.getDataInt("WriteTimeoutSec", WRITE_TIMEOUT_SEC_DEF)
	c.MaxHeaderBytes = c.getDataInt("MaxHeaderBytes", MAX_HEADER_BYTES_DEF)
	c.LogFilePath = c.getDataString("LogFilePath", LOG_FILE_PATH_DEF)

	if list := c.getDataStringArray("URLWhiteList", nil); list != nil {
		c.URLWhiteList = make([]*regexp.Regexp, 0, 1)
		for _, it := range list {
			c.URLWhiteList = append(c.URLWhiteList, regexp.MustCompile(it))
		}
	} else {
		c.URLWhiteList = nil
	}
	if list := c.getDataStringArray("BalanceList", nil); list != nil {
		c.BalanceList = make([]*Balance, 0, 1)
		for _, it := range list {
			if d := strings.Split(it, ":"); len(d) == 2 {
				num, err := strconv.ParseInt(d[1], 10, 16)
				if err != nil { break }
				c.BalanceList = append(c.BalanceList, &Balance{
					Host	: d[0],
					Port	: int(num),
				})
			} else {
				break
			}
		}
	}
	if len(c.BalanceList) == 0 {
		c.BalanceList = g_balance_def
	}
	return nil
}

func (c *Config) readDefault() {
	c.Addr = ADDR_DEF
	c.Port = PORT_DEF
	c.ReadTimeoutSec = READ_TIMEOUT_SEC_DEF
	c.WriteTimeoutSec = WRITE_TIMEOUT_SEC_DEF
	c.MaxHeaderBytes = MAX_HEADER_BYTES_DEF
	c.LogFilePath = LOG_FILE_PATH_DEF
	c.URLWhiteList = nil
	c.BalanceList = g_balance_def
}

func (c *Config) getDataInt(h string, def int) (ret int) {
	ret = def
	if it, ok := c.v[h]; ok {
		if f, err := it.(float64); err {
			ret = int(f)
		}
	}
	return
}

func (c *Config) getDataString(h, def string) (ret string) {
	ret = def
	if it, ok := c.v[h]; ok {
		if ret, ok = it.(string); !ok {
			ret = def
		}
	}
	return
}

func (c *Config) getDataStringArray(h string, def []string) (ret []string) {
	ret = def
	if inter, ok := c.v[h]; ok {
		if iterarr, ok := inter.([]interface{}); ok {
			ret = make([]string, 0, 1)
			for _, it := range iterarr {
				if s, ok := it.(string); ok {
					ret = append(ret, s)
				} else {
					ret = def
					break
				}
			}
		}
	}
	return
}

