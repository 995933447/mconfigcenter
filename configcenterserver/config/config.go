package config

import (
	"sync"
	"time"

	"github.com/995933447/easymicro/loader"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configimage"
	"github.com/995933447/natsevent"
)

const ServerConfigFileName = "configimageserver"

const (
	DefaultMongoConnName = "mconfigcenter_img"
	DefaultMongoDbName   = "mconfigcenter_img"
)

type ServerConfig struct {
	SamplePProfTimeLongSec              int    `mapstructure:"sample_pprof_time_long_sec"`
	Env                                 string `mapstructure:"env"`
	MongoDb                             string `mapstructure:"mongo_db"`
	MongoConn                           string `mapstructure:"mongo_conn"`
	ListenerGroup                       string `mapstructure:"listener_group"`
	SubNotificationNatsConn             string `mapstructure:"sub_notification_nats_conn"`
	SubNotificationNatsMaxAckWaitSec    int    `mapstructure:"sub_notification_max_ack_wait_sec"`
	SubNotificationNatsIdleHeartbeatSec int    `mapstructure:"sub_notification_nats_idle_heartbeat"`
	DiscoveryName                       string `mapstructure:"discovery_name"`
	ConfigCenterDiscoveryName           string `mapstructure:"config_center_discovery_name"`
}

func (c *ServerConfig) GetConfigCenterDiscoveryName() string {
	if c.ConfigCenterDiscoveryName == "" {
		return configcenter.EasymicroDiscoveryName
	}
	return c.ConfigCenterDiscoveryName
}

func (c *ServerConfig) GetDiscoveryName() string {
	if c.DiscoveryName == "" {
		return configimage.EasymicroDiscoveryName
	}
	return c.DiscoveryName
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

func (c *ServerConfig) GetMongoConn() string {
	if c.MongoConn == "" {
		return DefaultMongoConnName
	}
	return c.MongoConn
}

func (c *ServerConfig) GetMongoDb() string {
	if c.MongoDb == "" {
		return DefaultMongoDbName
	}
	return c.MongoDb
}

func (c *ServerConfig) GetSubNotificationNatsConn() string {
	if c.SubNotificationNatsConn == "" {
		return natsevent.ConnNameDefault
	}
	return c.SubNotificationNatsConn
}

func (c *ServerConfig) GetSubNotificationNatsMaxAckWait() time.Duration {
	return time.Duration(c.SubNotificationNatsMaxAckWaitSec) * time.Second
}

func (c *ServerConfig) GetSubNotificationNatsIdleHeartbeat() time.Duration {
	return time.Duration(c.SubNotificationNatsIdleHeartbeatSec) * time.Second
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

	if err = loader.LoadAndWatchRedisFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadAndWatchMongoFromLocal(); err != nil {
		return err
	}

	return nil
}
