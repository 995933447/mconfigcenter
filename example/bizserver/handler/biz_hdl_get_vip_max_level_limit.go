package handler

import (
	"context"

	"github.com/995933447/mconfigcenter/example/biz"
	"github.com/995933447/mconfigcenter/example/common/confighub/generalkv"
)

func (s *Biz) GetVipMaxLevelLimit(ctx context.Context, req *biz.GetVipMaxLevelLimitReq) (*biz.GetVipMaxLevelLimitResp, error) {
	var resp biz.GetVipMaxLevelLimitResp
	cfg, ok, err := generalkv.MustGetVipLevelMaxConfig().GetConfig()
	if err != nil {
		return nil, err
	}
	if !ok {
		return &resp, nil
	}
	resp.Level = int32(cfg.MaxLevel)
	return &resp, nil
}
