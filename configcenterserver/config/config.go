package config

import (
	"sync"

	"github.com/995933447/easymicro/loader"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/natsevent"
)

const ServerConfigFileName = "configcenterserver"

const (
	ListenerNotificationDirectness                   = ""
	ListenerNotificationDirectnessThroughImageServer = "throughImageServer"
	ListenerNotificationDirectnessDirect             = "direct"
)

type ServerConfig struct {
	SamplePProfTimeLongSec         int    `mapstructure:"sample_pprof_time_long_sec"`
	Env                            string `mapstructure:"env"`
	ListenerNotificationDirectness string `mapstructure:"listener_notification_direct"`
	MongoDb                        string `mapstructure:"mongo_db"`
	MongoConn                      string `mapstructure:"mongo_conn"`
	PubNotificationNatsConn        string `mapstructure:"pub_notification_nats_conn"`
	DiscoveryName                  string `mapstructure:"discovery_name"`
}

func (c *ServerConfig) GetDiscoveryName() string {
	if c.DiscoveryName == "" {
		return configcenter.EasymicroDiscoveryName
	}
	return c.DiscoveryName
}

func (c *ServerConfig) GetMongoConn() string {
	if c.MongoConn == "" {
		return configcenter.ConfigSchemaConnName
	}
	return c.MongoConn
}

func (c *ServerConfig) GetMongoDb() string {
	if c.MongoDb == "" {
		return configcenter.ConfigSchemaDbName
	}
	return c.MongoDb
}

func (c *ServerConfig) GetPubNotificationNatsConn() string {
	if c.PubNotificationNatsConn == "" {
		return natsevent.ConnNameDefault
	}
	return c.PubNotificationNatsConn
}

func (c *ServerConfig) IsDev() bool {
	return c.Env == "dev"
}

func (c *ServerConfig) IsTest() bool {
	return c.Env == "test"
}

func (c *ServerConfig) IsProd() bool {
	return c.Env == "prod"
}

var (
	serverConfig   ServerConfig
	serverConfigMu sync.RWMutex
)

func getServerConfig() *ServerConfig {
	return &serverConfig
}

func SafeReadServerConfig(fn func(c *ServerConfig)) {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()
	fn(getServerConfig())
}

func SafeWriteServerConfig(fn func(c *ServerConfig)) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()
	fn(getServerConfig())
}

func LoadConfig() error {
	err := loader.LoadFastlogFromLocal(nil)
	if err != nil {
		return err
	}

	err = loader.LoadAndWatchConfig(ServerConfigFileName, &serverConfig, &serverConfigMu, nil)
	if err != nil {
		return err
	}

	if err = loader.LoadEtcdFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadDiscoveryFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadNatsFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadAndWatchMongoFromLocal(); err != nil {
		return err
	}

	return nil
}
