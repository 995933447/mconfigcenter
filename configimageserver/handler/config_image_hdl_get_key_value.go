package handler

import (
	"context"
	"errors"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/common"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configimage"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *ConfigImage) GetKeyValue(ctx context.Context, req *configimage.GetKeyValueReq) (*configimage.GetKeyValueResp, error) {
	var resp configimage.GetKeyValueResp

	if req.Key == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "key is required")
	}

	config, err := s.newKVConfigModel().FindOneByKey(ctx, req.Key)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, grpc.NewRPCErr(configcenter.ErrCode_ErrCodeConfigNotFound)
		}

		fastlog.Error(err)
		return nil, err
	}

	resp.Config = &common.KVConfig{}
	resp.Config.Key = config.Key
	resp.Config.Value = config.Value
	resp.Config.Extra = config.Extra

	return &resp, nil
}
