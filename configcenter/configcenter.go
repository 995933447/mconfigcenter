package configcenter

import (
	"context"
	"errors"
	"sync"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/mconfigcenter/common"
	"github.com/995933447/mconfigcenter/configimage"
	"github.com/995933447/natsevent"
	"github.com/995933447/reconfmgr"
	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
)

func PrepareGRPC(ctx context.Context, discoveryName string, dialGRPCOpts ...grpc.DialOption) error {
	if discoveryName == "" {
		discoveryName = EasymicroDiscoveryName
	}

	if err := easymicrogrpc.PrepareDiscoverGRPC(ctx, EasymicroGRPCSchema, discoveryName); err != nil {
		return err
	}

	easymicrogrpc.RegisterServiceDialOpts(EasymicroGRPCPbServiceNameConfigCenter, true, dialGRPCOpts...)

	return nil
}

func InitReconfmgrReloader(listenerGroup string, opts ...natsevent.ApplySubOptsFunc) error {
	return natsevent.SubscribeBroadcast(EventNameConfigChanged, func(evt *ConfigChangedEvent) error {
		if listenerGroup != evt.RefreshListenerGroup {
			return nil
		}

		var keys []string
		if evt.ShouldReloadAll {
			keys = append(keys, "*")
		} else {
			for _, config := range evt.Configs {
				keys = append(keys, config.CollName)
			}
		}

		reconfmgr.Reload(keys)

		return nil
	}, opts...)
}

const KVConfigName = "mconfigcenter.KVConfig"

type KVConfigDataSrc int

const (
	KVConfigDataSrcNil KVConfigDataSrc = iota
	KVConfigDataSrcLocalImage
	KVConfigDataSrcConfigImage
	KVConfigDataSrcConfigCenter
)

func RegisterKVConfig(priority int, listenKeys []string, mgoConnName, mgoDbName string, dataSrc KVConfigDataSrc) error {
	var hasListenCollName bool
	for _, key := range listenKeys {
		if key == common.KVConfigDbName {
			hasListenCollName = true
			break
		}
	}
	if !hasListenCollName {
		listenKeys = append(listenKeys, common.KVConfigTbName)
	}
	return reconfmgr.Register(KVConfigName, &KVConf{
		priority:    priority,
		listenKeys:  listenKeys,
		mgoConnName: mgoConnName,
		mgoDbName:   mgoDbName,
		dataSrc:     dataSrc,
	})
}

var _ reconfmgr.Config = (*KVConf)(nil)

type KVConf struct {
	reconfmgr.ConfigBase
	priority    int
	listenKeys  []string
	mgoConnName string
	mgoDbName   string
	kvs         *sync.Map
	dataSrc     KVConfigDataSrc
}

func (c *KVConf) LoadConfig() error {
	var configs []*common.KVConfigOrm

	switch c.dataSrc {
	case KVConfigDataSrcConfigCenter:
		listConfigResp, err := ConfigCenterGRPC().ListConfig(context.TODO(), &ListConfigReq{
			CollName: common.KVConfigTbName,
		})
		if err != nil {
			return err
		}

		for _, item := range listConfigResp.List {
			var config common.KVConfigOrm
			if err := bson.Unmarshal(item, &config); err != nil {
				return err
			}
			configs = append(configs, &config)
		}
	case KVConfigDataSrcConfigImage:
		listConfigResp, err := configimage.ConfigImageGRPC().ListConfig(context.TODO(), &configimage.ListConfigReq{
			CollName: common.KVConfigTbName,
		})
		if err != nil {
			return err
		}

		for _, item := range listConfigResp.List {
			var config common.KVConfigOrm
			if err := bson.Unmarshal(item, &config); err != nil {
				return err
			}
			configs = append(configs, &config)
		}
	case KVConfigDataSrcLocalImage:
		mod := common.NewKVConfigModel()
		if c.mgoConnName != "" {
			mod.SetConn(c.mgoConnName)
		}
		if c.mgoDbName != "" {
			mod.SetDb(c.mgoDbName)
		}

		var err error
		configs, err = mod.FindAll(context.TODO(), bson.M{})
		if errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
	default:
		return errors.New("unknown data source")
	}

	var kvs sync.Map
	for _, config := range configs {
		kvs.Store(config.Key, config)
	}

	c.kvs = &kvs

	ReloadKVSubConfig()

	return nil
}

func (c *KVConf) GetConfig(key string) (*common.KVConfigOrm, bool) {
	res, ok := c.kvs.Load(key)
	if ok {
		return res.(*common.KVConfigOrm), ok
	}
	return nil, false
}

func (c *KVConf) GetValue(key string, val any) (bool, error) {
	config, ok := c.GetConfig(key)
	if !ok {
		return false, nil
	}

	switch val.(type) {
	case *string:
		val = &config.Value
	default:
		if err := jsoniter.UnmarshalFromString(config.Value, val); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (c *KVConf) GetListenKeys() []string {
	return c.listenKeys
}

func (c *KVConf) GetPriority() int {
	return c.priority
}

type KVSubConfig interface {
	LoadConfig()
}

var kvSubConfigs sync.Map

func ReloadKVSubConfig() {
	kvSubConfigs.Range(func(key, value any) bool {
		if subConf, ok := value.(KVSubConfig); ok {
			subConf.LoadConfig()
		}
		return true
	})
}

func RegisterKVSubConfig(key string, subConf KVSubConfig) {
	kvSubConfigs.Store(key, subConf)
}

func GetKVSubConfig(key string) (KVSubConfig, bool) {
	config, ok := kvSubConfigs.Load(key)
	if !ok {
		return nil, false
	}
	return config.(KVSubConfig), true
}

type KVSubConfigWrapper[T any] struct {
	cache      *T
	mu         sync.RWMutex
	Key        string
	InitConfig func(config *T)
}

// GetConfig 获取配置
func (c *KVSubConfigWrapper[T]) GetConfig() (*T, bool, error) {
	c.mu.RLock()
	if c.cache != nil {
		c.mu.RUnlock()
		return c.cache, true, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	var cfg T
	ok, err := reconfmgr.MustGet(KVConfigName).(*KVConf).GetValue(c.Key, &cfg)
	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, nil
	}

	c.cache = &cfg

	if c.InitConfig != nil {
		c.InitConfig(&cfg)
	}

	return &cfg, true, nil
}

func (c *KVSubConfigWrapper[T]) LoadConfig() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = nil
}
