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

func (s *ConfigCenter) AddConfig(ctx context.Context, req *configcenter.AddConfigReq) (*configcenter.AddConfigResp, error) {
	var resp configcenter.AddConfigResp

	if req.Value == nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "value is empty")
	}

	if req.CollName == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "collName is empty")
	}

	m := bson.M{}
	err := bson.Unmarshal(req.Value, m)
	if err != nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("value parse failed, err:%v", err))
	}

	var id primitive.ObjectID
	if idStr, ok := m["_id"]; !ok {
		id = primitive.NewObjectID()
	} else {
		id, err = primitive.ObjectIDFromHex(idStr.(string))
		if err != nil || id.IsZero() {
			id = primitive.NewObjectID()
		}
		delete(m, "_id")
	}

	if err = s.validateSchema(ctx, req.CollName, m); err != nil {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeValidateSchemaFailed, err.Error())
	}

	m["created_at"] = time.Now()
	m["updated_at"] = time.Now()
	m["_id"] = id

	_, err = s.newGeneralModel(req.CollName).InsertOne(ctx, m)
	if err != nil {
		fastlog.Errorf("insert value:%+v to collection:%s error:%+v", m, req.CollName, err)
		return nil, err
	}

	if req.ShouldNotifyListeners {
		if err = s.pubConfigChangedEvt(req.CollName, req.ListenerGroup, id.Hex()); err != nil {
			fastlog.Errorf("publish config changed event failed, err:%v", err)
			return nil, err
		}
	}

	return &resp, nil
}
