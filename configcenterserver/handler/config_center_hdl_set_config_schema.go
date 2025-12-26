package handler

import (
	"context"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configcenterserver/config"
	"github.com/995933447/mgorm"
	"github.com/995933447/runtimeutil"
	"go.mongodb.org/mongo-driver/bson"
)

func (s *ConfigCenter) SetConfigSchema(ctx context.Context, req *configcenter.SetConfigSchemaReq) (*configcenter.SetConfigSchemaResp, error) {
	var resp configcenter.SetConfigSchemaResp

	if req.Schema == nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "schema is required")
	}

	if req.Schema.CollName == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "schema.coll_name is required")
	}

	var schema configcenter.ConfigSchemaOrm
	schema.JsonSchema = req.Schema.JsonSchema
	schema.CollName = req.Schema.CollName
	schema.IndexKeys = req.Schema.IndexKeys
	schema.UniqIndexKeys = req.Schema.UniqIndexKeys
	schema.Desc = req.Schema.Desc

	update, err := mgorm.ToBsonM(schema)
	if err != nil {
		fastlog.Error(err)
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, err.Error())
	}

	delete(update, "_id")
	delete(update, "created_at")

	if _, err = s.newConfigSchemaModel().Upsert(ctx, bson.M{"coll_name": req.Schema.CollName}, update); err != nil {
		fastlog.Error(err)
		return nil, err
	}

	if req.AsyncCreateIndexes {
		runtimeutil.Go(func() {
			err = s.createConfigCollIdxes(context.TODO(), req.Schema.CollName, req.Schema.IndexKeys, req.Schema.UniqIndexKeys)
			if err != nil {
				fastlog.Error(err)
			}
		})
	} else {
		err = s.createConfigCollIdxes(ctx, req.Schema.CollName, req.Schema.IndexKeys, req.Schema.UniqIndexKeys)
		if err != nil {
			fastlog.Error(err)
			return nil, err
		}
	}

	return &resp, nil
}

func (s *ConfigCenter) createConfigCollIdxes(ctx context.Context, collName string, indexKeys []string, uniqIndexKeys []string) error {
	var conn, db string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		conn = c.GetMongoConn()
		db = c.GetMongoDb()
	})
	err := mgorm.NewOrm(conn, db, collName, false, nil, indexKeys, uniqIndexKeys, nil).CreatIndexes(ctx)
	if err != nil {
		fastlog.Errorf("create config collection:%s failed, err:%v", collName, err)
		return err
	}
	return nil
}
