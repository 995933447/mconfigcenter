package handler

import (
	"github.com/995933447/mconfigcenter/common"
	"github.com/995933447/mconfigcenter/configimage"
	"github.com/995933447/mconfigcenter/configimageserver/config"
	"github.com/995933447/mgorm"
)

type ConfigImage struct {
	configimage.UnimplementedConfigImageServer
	ServiceName string
}

var ConfigImageHandler = &ConfigImage{
	ServiceName: configimage.EasymicroGRPCPbServiceNameConfigImage,
}

func (s *ConfigImage) newKVConfigModel() *common.KVConfigModel {
	mod := common.NewKVConfigModel()
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		mod.SetConn(c.GetMongoConn())
		mod.SetDb(c.GetMongoDb())
	})
	return mod
}

func (s *ConfigImage) newGeneralModel(collName string) *mgorm.Orm {
	var conn, db string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		conn = c.GetMongoConn()
		db = c.GetMongoDb()
	})

	return mgorm.NewOrm(
		conn,
		db,
		collName,
		false,
		nil,
		nil,
		nil,
		nil,
	)
}
