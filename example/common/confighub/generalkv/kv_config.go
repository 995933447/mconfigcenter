package generalkv

import "github.com/995933447/mconfigcenter/configcenter"

func RegisterKVConfig() error {
	err := configcenter.RegisterKVConfig(999, nil, "", "", configcenter.KVConfigDataSrcConfigImage)
	if err != nil {
		return err
	}
	return nil
}
