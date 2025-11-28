package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (s *ConfigCenter) UpdateConfig(ctx context.Context, req *configcenter.UpdateConfigReq) (*configcenter.UpdateConfigResp, error) {
	var resp configcenter.UpdateConfigResp

	if req.Id == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "id is required")
	}

	if req.Value == nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "value is required")
	}

	m := bson.M{}
	err := bson.Unmarshal(req.Value, m)
	if err != nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("value parse failed, err:%v", err))
	}

	delete(m, "_id")

	if err = s.validateSchema(ctx, req.CollName, m); err != nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeValidateSchemaFailed, err.Error())
	}

	idObj, err := primitive.ObjectIDFromHex(req.Id)
	if err != nil {
		fastlog.Error(err)
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, err.Error())
	}

	filter := bson.M{"_id": idObj}
	m["updated_at"] = time.Now()

	res, err := s.newGeneralModel(req.CollName).UpdateOne(ctx, filter, bson.M{"$set": m})
	if err != nil {
		fastlog.Errorf("update config to collection failed, err:%v", err)
		return nil, err
	}

	if res.MatchedCount == 0 {
		return &resp, nil
	}

	if req.ShouldNotifyListeners {
		if err = s.pubConfigChangedEvt(req.CollName, req.ListenerGroup, req.Id); err != nil {
			fastlog.Errorf("publish config changed event failed, err:%v", err)
			return nil, err
		}
	}

	return &resp, nil
}
