package doge

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

type Config struct {
	IsEnabled       bool   `yaml:"is-enable"`
	IsUserEnabled   bool   `yaml:"is-user-enable"`
	BucketName      string `yaml:"bucket-name"`
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"access-key-id"`
	AccessKeySecret string `yaml:"access-key-secret"`
	CustomPath      string `yaml:"custom-path"`
}

// TmpTokenResponse 多吉云临时凭证响应
type TmpTokenResponse struct {
	Code int                  `json:"code"`
	Msg  string               `json:"msg"`
	Data TmpTokenResponseData `json:"data,omitempty"`
}

// TmpTokenResponseData 临时凭证数据
type TmpTokenResponseData struct {
	Credentials Credentials `json:"Credentials"`
	ExpiredAt   int         `json:"ExpiredAt"`
}

// Credentials 临时凭证
type Credentials struct {
	AccessKeyId     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
}

type Doge struct {
	S3Client *s3.Client
	Config   *Config

	mu         sync.RWMutex
	stopCron   chan struct{}
	cronClosed bool
}

var clients = make(map[string]*Doge)
var clientsMu sync.Mutex

func NewClient(cf map[string]any) (*Doge, error) {
	var IsEnabled bool
	switch t := cf["IsEnabled"].(type) {
	case int64:
		IsEnabled = t != 0
	case bool:
		IsEnabled = t
	}

	var IsUserEnabled bool
	switch t := cf["IsUserEnabled"].(type) {
	case int64:
		IsUserEnabled = t != 0
	case bool:
		IsUserEnabled = t
	}

	conf := &Config{
		IsEnabled:       IsEnabled,
		IsUserEnabled:   IsUserEnabled,
		Endpoint:        cf["Endpoint"].(string),
		Region:          cf["Region"].(string),
		BucketName:      cf["BucketName"].(string),
		AccessKeyID:     cf["AccessKeyID"].(string),
		AccessKeySecret: cf["AccessKeySecret"].(string),
		CustomPath:      cf["CustomPath"].(string),
	}

	clientsMu.Lock()
	defer clientsMu.Unlock()

	if clients[conf.AccessKeyID] != nil {
		return clients[conf.AccessKeyID], nil
	}

	d := &Doge{
		Config:   conf,
		stopCron: make(chan struct{}),
	}

	// 初始化 S3 客户端
	if err := d.initSession(); err != nil {
		return nil, err
	}

	// 启动定时刷新凭证的协程
	// 多吉云每次临时生成的秘钥有效期为 2h，这里设置为 118 分钟重新生成一次
	go d.startCredentialRefresher()

	clients[conf.AccessKeyID] = d
	return d, nil
}

// getCredentials 从多吉云 API 获取临时凭证
func getCredentials(accessKey, secretKey string) (Credentials, error) {
	apiPath := "/auth/tmp_token.json"
	reqBody, err := json.Marshal(map[string]interface{}{"channel": "OSS_FULL", "scopes": []string{"*"}})
	if err != nil {
		return Credentials{}, err
	}

	signStr := apiPath + "\n" + string(reqBody)
	hmacObj := hmac.New(sha1.New, []byte(secretKey))
	hmacObj.Write([]byte(signStr))
	sign := hex.EncodeToString(hmacObj.Sum(nil))
	authorization := "TOKEN " + accessKey + ":" + sign

	req, err := http.NewRequest("POST", "https://api.dogecloud.com"+apiPath, strings.NewReader(string(reqBody)))
	if err != nil {
		return Credentials{}, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", authorization)

	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Credentials{}, err
	}
	defer resp.Body.Close()

	ret, err := io.ReadAll(resp.Body)
	if err != nil {
		return Credentials{}, err
	}

	var tmpTokenResp TmpTokenResponse
	if err := json.Unmarshal(ret, &tmpTokenResp); err != nil {
		return Credentials{}, err
	}

	if tmpTokenResp.Code != 200 {
		return Credentials{}, errors.Errorf("doge api error: %s", tmpTokenResp.Msg)
	}

	return tmpTokenResp.Data.Credentials, nil
}

// initSession 初始化 S3 会话
func (d *Doge) initSession() error {
	// 获取临时凭证
	creds, err := getCredentials(d.Config.AccessKeyID, d.Config.AccessKeySecret)
	if err != nil {
		return errors.Wrap(err, "doge: failed to get credentials")
	}

	region := d.Config.Region
	if region == "" {
		region = "automatic"
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyId,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
		config.WithRegion(region),
	)
	if err != nil {
		return errors.Wrap(err, "doge: failed to load config")
	}

	endpoint := d.Config.Endpoint
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})

	d.mu.Lock()
	d.S3Client = client
	d.mu.Unlock()

	return nil
}

// startCredentialRefresher 启动凭证刷新协程
func (d *Doge) startCredentialRefresher() {
	ticker := time.NewTicker(118 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := d.initSession(); err != nil {
				// 记录错误但继续运行
				continue
			}
		case <-d.stopCron:
			return
		}
	}
}

// Close 关闭客户端并停止凭证刷新
func (d *Doge) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.cronClosed {
		close(d.stopCron)
		d.cronClosed = true
	}
}

// getClient 获取 S3 客户端（线程安全）
func (d *Doge) getClient() *s3.Client {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.S3Client
}
