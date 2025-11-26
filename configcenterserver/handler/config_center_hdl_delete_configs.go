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

func (s *ConfigCenter) DeleteConfigs(ctx context.Context, req *configcenter.DeleteConfigsReq) (*configcenter.DeleteConfigsResp, error) {
	var resp configcenter.DeleteConfigsResp

	if len(req.Ids) == 0 {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "ids is required")
	}

	var objIds []primitive.ObjectID
	for _, id := range req.Ids {
		objId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("id parse failed, err:%v", err))
		}

		objIds = append(objIds, objId)
	}

	res, err := s.newGeneralModel(req.CollName).DeleteMany(ctx, bson.M{"_id": bson.M{"$in": objIds}})
	if err != nil {
		fastlog.Errorf("delete config from collection:%s by ids:%s error:%+v", req.CollName, req.Ids, err)
		return nil, err
	}

	if res.DeletedCount == 0 {
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
