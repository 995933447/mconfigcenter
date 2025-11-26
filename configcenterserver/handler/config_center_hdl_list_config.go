package handler

import (
	"context"
	"fmt"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *ConfigCenter) ListConfig(ctx context.Context, req *configcenter.ListConfigReq) (*configcenter.ListConfigResp, error) {
	var resp configcenter.ListConfigResp

	if req.CollName == "" {
		return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, "CollName is required")
	}

	var err error
	filter := bson.M{}
	if req.Filter != nil {
		err = bson.Unmarshal(req.Filter, filter)
		if err != nil {
			return nil, grpc.NewRPCErrWithMsg(configcenter.ErrCode_ErrCodeParamInvalid, fmt.Sprintf("filter parse failed, err:%v", err))
		}
	}

	sorts := bson.D{}
	if len(req.Sorts) > 0 {
		for _, v := range req.Sorts {
			sorts = append(sorts, bson.E{Key: v.Field, Value: v.SortWay})
		}
	}

	selectors := bson.M{}
	for _, selector := range req.Selectors {
		selectors[selector.Field] = selector.Selected
	}

	mod := s.newGeneralModel(req.CollName)

	var cursor *mongo.Cursor
	if req.Limit > 0 || req.Offset > 0 {
		cursor, err = mod.FindManyByPage(ctx, filter, sorts, int64(req.Offset), int64(req.Limit), selectors)
		if err != nil {
			fastlog.Errorf("list config find by page failed, err:%v", err)
			return nil, err
		}
	} else {
		cursor, err = mod.FindAll(ctx, filter, sorts, selectors)
		if err != nil {
			fastlog.Errorf("list config find all failed, err:%v", err)
			return nil, err
		}
	}

	for cursor.Next(ctx) {
		item := make(map[string]interface{})
		if err := cursor.Decode(&item); err != nil {
			fastlog.Errorf("decode mongo item failed, err:%v", err)
			return nil, err
		}

		b, err := bson.Marshal(item)
		if err != nil {
			fastlog.Errorf("encode mongo item failed, err:%v", err)
			return nil, err
		}

		resp.List = append(resp.List, b)
	}

	total, err := mod.FindCount(ctx, filter)
	if err != nil {
		fastlog.Errorf("list config find count failed, err:%v", err)
		return nil, err
	}

	resp.Total = uint32(total)

	return &resp, nil
}
