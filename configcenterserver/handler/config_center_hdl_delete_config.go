package handler

import (
	"context"
	"fmt"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (s *ConfigCenter) DeleteConfig(ctx context.Context, req *configcenter.DeleteConfigReq) (*configcenter.DeleteConfigResp, error) {
	var resp configcenter.DeleteConfigResp

	if req.Id == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "id is required")
	}

	idObj, err := primitive.ObjectIDFromHex(req.Id)
	if err != nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("id parse failed, err:%v", err))
	}

	res, err := s.newGeneralModel(req.CollName).DeleteOne(ctx, bson.M{"_id": idObj})
	if err != nil {
		fastlog.Errorf("delete config from collection:%s by id:%s error:%+v", req.CollName, req.Id, err)
		return nil, err
	}

	if res.DeletedCount == 0 {
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
