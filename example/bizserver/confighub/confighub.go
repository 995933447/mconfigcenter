package confighub

import (
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/example/common/confighub/generalkv"
)

func LoadConfigHub(listenerGroup string) error {
	if err := configcenter.InitReconfmgrReloader(listenerGroup); err != nil {
		return err
	}
	if err := generalkv.RegisterKVConfig(); err != nil {
		return err
	}
	generalkv.RegisterVipLevelMaxConfig()
	return nil
}
