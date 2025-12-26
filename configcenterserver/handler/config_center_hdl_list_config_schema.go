package handler

import (
	"context"

	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
	"go.mongodb.org/mongo-driver/bson"
)

func (s *ConfigCenter) ListConfigSchema(ctx context.Context, req *configcenter.ListConfigSchemaReq) (*configcenter.ListConfigSchemaResp, error) {
	var resp configcenter.ListConfigSchemaResp

	configSchemas, err := s.newConfigSchemaModel().FindAll(ctx, bson.M{})
	if err != nil {
		fastlog.Errorf("ListConfigSchema err: %v", err)
		return nil, err
	}

	for _, configSchema := range configSchemas {
		resp.List = append(resp.List, &configcenter.ConfigSchema{
			CollName:      configSchema.CollName,
			IndexKeys:     configSchema.IndexKeys,
			UniqIndexKeys: configSchema.UniqIndexKeys,
			JsonSchema:    configSchema.JsonSchema,
			Desc:          configSchema.Desc,
		})
	}

	return &resp, nil
}
