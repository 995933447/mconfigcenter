package event

import (
	"context"
	"time"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configimageserver/config"
	"github.com/995933447/mgorm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func OnConfigChangedEvent(evt *configcenter.ConfigChangedEvent) error {
	fastlog.PrintInfo("received config changed event", evt)

	var currListenerGroup string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		currListenerGroup = c.ListenerGroup
	})
	if evt.RefreshListenerGroup != currListenerGroup {
		fastlog.Infof("not my listener group: %s, skipped sync config", evt.RefreshListenerGroup)
		return nil
	}

	if len(evt.Configs) == 0 && !evt.ShouldReloadAll {
		fastlog.Info("not config changed, skipped sync config")
		return nil
	}

	var configs []*configcenter.ConfigChangedEventConfig
	if !evt.ShouldReloadAll {
		configs = evt.Configs
	} else {
		var connName, dbName string
		config.SafeReadServerConfig(func(c *config.ServerConfig) {
			connName = c.GetMongoConn()
			dbName = c.GetMongoDb()
		})

		conn, err := mgorm.GetClient(connName)
		if err != nil {
			fastlog.Errorf("get mgorm client, err:%v", err)
			return err
		}

		collNames, err := conn.Database(dbName).ListCollectionNames(context.Background(), mongo.Pipeline{})
		if err != nil {
			fastlog.Errorf("Get collections failed: %v", err)
			return err
		}

		for _, collName := range collNames {
			configs = append(configs, &configcenter.ConfigChangedEventConfig{
				CollName: collName,
			})
		}
	}

	for _, c := range configs {
		if len(c.ConfigIds) == 0 {
			start := time.Now()
			fastlog.Infof("start sync config all, collection: %s", c.CollName)
			err := syncConfigAll(c.CollName)
			if err != nil {
				fastlog.Errorf("sync config collection:%s all, err:%v", c.CollName, err)
				return err
			}
			fastlog.Infof("finish sync config all, collection: %s, cost:%s", c.CollName, time.Since(start))
			continue
		}

		err := syncConfig(c.CollName, c.ConfigIds)
		if err != nil {
			fastlog.Errorf("sync config collection:%s all, err:%v", c.CollName, err)
			return err
		}
	}

	if err := evt.Send(); err != nil {
		fastlog.Errorf("send config changed event failed, err:%v", err)
		return err
	}

	return nil
}

func syncConfig(collName string, configIds []string) error {
	var indexKeys, uniqIndexKeys []string
	getConfigSchema, err := configcenter.ConfigCenterGRPC().GetConfigSchema(context.TODO(), &configcenter.GetConfigSchemaReq{
		CollName: collName,
	})
	if err != nil {
		if !grpc.IsRPCErr(err, configcenter.ErrCode_ErrCodeConfigSchemaNotFound.Number()) {
			fastlog.Errorf("GetConfigSchema for collection:%s , err:%v", collName, err)
			return err
		}
	} else {
		indexKeys = getConfigSchema.Schema.IndexKeys
		uniqIndexKeys = getConfigSchema.Schema.UniqIndexKeys
	}

	var connName, dbName string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		connName = c.GetMongoConn()
		dbName = c.GetMongoDb()
	})

	mod := mgorm.NewOrm(
		connName,
		dbName,
		collName,
		false,
		nil,
		indexKeys,
		uniqIndexKeys,
		nil,
	)

	var idObjs []primitive.ObjectID
	for _, configId := range configIds {
		idObj, err := primitive.ObjectIDFromHex(configId)
		if err != nil {
			return err
		}
		idObjs = append(idObjs, idObj)
	}

	filter := bson.M{
		"_id": bson.M{
			"$in": idObjs,
		},
	}

	b, err := bson.Marshal(filter)
	if err != nil {
		fastlog.Errorf("marshal filter failed, err:%v", err)
		return err
	}

	listResp, err := configcenter.ConfigCenterGRPC().ListConfig(context.TODO(), &configcenter.ListConfigReq{
		CollName: collName,
		Offset:   0,
		Limit:    0,
		Filter:   b,
	})
	if err != nil {
		fastlog.Errorf("ConfigCenterGRPC.ListConfig, err:%v", err)
		return err
	}

	// 没有配置，说明被删除了
	if len(listResp.List) == 0 {
		_, err = mod.DeleteMany(context.TODO(), filter)
		if err != nil {
			fastlog.Errorf("DeleteMany from collection:%s err:%v", collName, err)
			return err
		}

		return nil
	}

	for _, v := range listResp.List {
		m := bson.M{}
		err := bson.Unmarshal(v, m)
		if err != nil {
			fastlog.Errorf("mgorm.ToBsonM err:%v", err)
			return err
		}

		idAny, ok := m["_id"]
		if !ok {
			fastlog.Errorf("get item id of ConfigCenterGRPC.ListConfig from %+v failed", m)
			continue
		}

		id, ok := idAny.(primitive.ObjectID)
		if !ok {
			fastlog.Errorf("get item id of ConfigCenterGRPC.ListConfig from %+v failed", m)
			continue
		}

		_, err = mod.Upsert(context.TODO(), bson.M{
			"_id": id,
		}, bson.M{"$set": m})
		if err != nil {
			fastlog.Errorf("UpsertMany from collection:%s err:%v", collName, err)
			return err
		}
	}

	return nil
}

func syncConfigAll(collName string) error {
	var indexKeys, uniqIndexKeys []string
	getConfigSchema, err := configcenter.ConfigCenterGRPC().GetConfigSchema(context.TODO(), &configcenter.GetConfigSchemaReq{
		CollName: collName,
	})
	if err != nil {
		if !grpc.IsRPCErr(err, configcenter.ErrCode_ErrCodeConfigSchemaNotFound.Number()) {
			fastlog.Errorf("GetConfigSchema for collection:%s , err:%v", collName, err)
			return err
		}
	} else {
		indexKeys = getConfigSchema.Schema.IndexKeys
		uniqIndexKeys = getConfigSchema.Schema.UniqIndexKeys
	}

	var connName, dbName string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		connName = c.GetMongoConn()
		dbName = c.GetMongoDb()
	})

	tempCollName := collName + "_temp_" + time.Now().Format("20060102150405")
	mod := mgorm.NewOrm(
		connName,
		dbName,
		tempCollName,
		false,
		nil,
		indexKeys,
		uniqIndexKeys,
		nil,
	)

	var offset uint32
	for {
		listConfigResp, err := configcenter.ConfigCenterGRPC().ListConfig(context.TODO(), &configcenter.ListConfigReq{
			CollName: collName,
			Offset:   offset,
			Limit:    1000,
			Sorts: []*configcenter.ListConfigReq_Sort{
				{
					Field:   "_id",
					SortWay: 1,
				},
			},
		})
		if err != nil {
			fastlog.Errorf("ConfigCenterGRPC.ListConfig, err:%v", err)
			return err
		}

		if len(listConfigResp.List) == 0 {
			break
		}

		var values []any
		for _, b := range listConfigResp.List {
			m := bson.M{}
			err := bson.Unmarshal(b, m)
			if err != nil {
				fastlog.Errorf("decode config failed, err:%v", err)
				return err
			}

			values = append(values, m)
		}

		_, err = mod.InsertMany(context.TODO(), values)
		if err != nil {
			fastlog.Errorf("insert configs failed, err:%v", err)
			return err
		}

		offset += uint32(len(listConfigResp.List))
	}

	// rename 覆盖
	fullTempCollName := dbName + "." + tempCollName
	fullCollName := dbName + "." + collName
	cmd := bson.D{
		{"renameCollection", fullTempCollName},
		{"to", fullCollName},
		{"dropTarget", true},
	}

	client, err := mgorm.GetClient(connName)
	if err != nil {
		fastlog.Errorf("get mgorm client, err:%v", err)
		return err
	}

	adminDb := client.Database("admin")
	if err = adminDb.RunCommand(context.TODO(), cmd).Err(); err != nil {
		fastlog.Errorf("execute admin command: rename %s -> %s failed, err:%v", fullTempCollName, fullCollName, err)
		return err
	}

	return nil
}
