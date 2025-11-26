package handler

import (
	"context"
	"errors"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/common"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mgorm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *ConfigCenter) SetKeyValue(ctx context.Context, req *configcenter.SetKeyValueReq) (*configcenter.SetKeyValueResp, error) {
	var resp configcenter.SetKeyValueResp

	if req.Config == nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "config required")
	}

	if req.Config.Key == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "config.key is required")
	}

	if req.Config.Value == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "config.value is required")
	}

	config := &common.KVConfigOrm{
		Key:   req.Config.Key,
		Value: req.Config.Value,
		Extra: req.Config.Extra,
	}

	value, err := mgorm.ToBsonM(config)
	if err != nil {
		fastlog.Error(err)
		return nil, err
	}

	mod := s.newKVConfigModel()
	res, err := mod.Upsert(ctx, bson.M{"key": req.Config.Key}, value)
	if err != nil {
		fastlog.Error(err)
		return nil, err
	}

	if res.UpsertedCount == 0 && res.ModifiedCount == 0 {
		return &resp, nil
	}

	var id string
	if res.UpsertedID != nil {
		id = res.UpsertedID.(primitive.ObjectID).Hex()
	} else {
		config, err = mod.FindOneByKey(ctx, req.Config.Key)
		if err != nil {
			if !errors.Is(err, mongo.ErrNoDocuments) {
				fastlog.Error(err)
				return nil, err
			}
		} else {
			id = config.ID.Hex()
		}
	}

	if req.ShouldNotifyListeners {
		if err = s.pubConfigChangedEvt(common.KVConfigTbName, req.ListenerGroup, id); err != nil {
			fastlog.Errorf("publish config changed event failed, err:%v", err)
			return nil, err
		}
	}

	return &resp, nil
}
