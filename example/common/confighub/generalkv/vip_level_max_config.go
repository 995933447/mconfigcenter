package generalkv

import "github.com/995933447/mconfigcenter/configcenter"

type VipLevelMax struct {
	MaxLevel int `json:"max_level"`
}

type VipLevelMaxConfig struct {
	configcenter.KVSubConfigWrapper[VipLevelMax]
}

func RegisterVipLevelMaxConfig() {
	cfg := &VipLevelMaxConfig{}
	cfg.InitConfig = func(config *VipLevelMax) {
		if config.MaxLevel < 20 {
			config.MaxLevel = 20
		}
	}
	cfg.Key = KeyVipLevelMaxConfig
	configcenter.RegisterKVSubConfig(KeyVipLevelMaxConfig, cfg)
}

func MustGetVipLevelMaxConfig() *VipLevelMaxConfig {
	cfg, ok := configcenter.GetKVSubConfig(KeyVipLevelMaxConfig)
	if !ok {
		panic("configcenter not register VipLevelMaxConfig")
	}
	return cfg.(*VipLevelMaxConfig)
}
