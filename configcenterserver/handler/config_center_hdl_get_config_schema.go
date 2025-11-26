package handler

import (
	"context"
	"errors"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *ConfigCenter) GetConfigSchema(ctx context.Context, req *configcenter.GetConfigSchemaReq) (*configcenter.GetConfigSchemaResp, error) {
	var resp configcenter.GetConfigSchemaResp

	schema, err := s.newConfigSchemaModel().FindOneByCollName(ctx, req.CollName)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, grpc.NewRPCErr(configcenter.ErrCode_ErrCodeConfigSchemaNotFound)
		}
		fastlog.Errorf("find schema from mongo failed, err:%v", err)
		return nil, err
	}

	resp.Schema = &configcenter.ConfigSchema{}
	resp.Schema.JsonSchema = schema.JsonSchema
	resp.Schema.IndexKeys = schema.IndexKeys
	resp.Schema.UniqIndexKeys = schema.UniqIndexKeys
	resp.Schema.CollName = schema.CollName

	return &resp, nil
}
