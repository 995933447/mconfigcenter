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

func (s *ConfigCenter) AddConfigs(ctx context.Context, req *configcenter.AddConfigsReq) (*configcenter.AddConfigsResp, error) {
	var resp configcenter.AddConfigsResp

	if len(req.Values) == 0 {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "value is empty")
	}

	if req.CollName == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "collName is empty")
	}

	var (
		values []any
		ids    []string
	)
	for _, v := range req.Values {
		m := bson.M{}
		
		err := bson.Unmarshal(v, m)
		if err != nil {
			return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("value parse failed, err:%v", err))
		}

		if err = s.validateSchema(ctx, req.CollName, m); err != nil {
			return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeValidateSchemaFailed, err.Error())
		}

		m["created_at"] = time.Now()
		m["updated_at"] = time.Now()

		var objId primitive.ObjectID
		if idStr, ok := m["_id"]; !ok {
			objId = primitive.NewObjectID()
			m["_id"] = objId
		} else {
			objId, err = primitive.ObjectIDFromHex(idStr.(string))
			if err != nil || objId.IsZero() {
				objId = primitive.NewObjectID()
				m["_id"] = objId
			} else {
				m["_id"] = objId
			}
		}

		ids = append(ids, objId.Hex())
		values = append(values, m)
	}

	_, err := s.newGeneralModel(req.CollName).InsertMany(ctx, values)
	if err != nil {
		fastlog.Errorf("insert value:%+v to collection:%s error:%+v", values, req.CollName, err)
		return nil, err
	}

	if req.ShouldNotifyListeners {
		if err = s.pubConfigChangedEvt(req.CollName, req.ListenerGroup, ids...); err != nil {
			fastlog.Errorf("publish config changed event failed, err:%v", err)
			return nil, err
		}
	}
	return &resp, nil
}
