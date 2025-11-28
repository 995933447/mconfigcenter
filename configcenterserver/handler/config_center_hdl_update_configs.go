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

func (s *ConfigCenter) UpdateConfigs(ctx context.Context, req *configcenter.UpdateConfigsReq) (*configcenter.UpdateConfigsResp, error) {
	var resp configcenter.UpdateConfigsResp

	var objIds []primitive.ObjectID
	for _, id := range req.Ids {
		objId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("id parse failed, err:%v", err))
		}

		objIds = append(objIds, objId)
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

	filter := bson.M{"_id": bson.M{"$in": objIds}}
	m["updated_at"] = time.Now()

	res, err := s.newGeneralModel(req.CollName).UpdateMany(ctx, filter, bson.M{"$set": m})
	if err != nil {
		fastlog.Errorf("update config to collection failed, err:%v", err)
		return nil, err
	}

	if res.MatchedCount == 0 {
		return &resp, nil
	}

	if req.ShouldNotifyListeners {
		if err = s.pubConfigChangedEvt(req.CollName, req.ListenerGroup, req.Ids...); err != nil {
			fastlog.Errorf("publish config changed event failed, err:%v", err)
			return nil, err
		}
	}

	return &resp, nil
}
