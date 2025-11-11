package handler

import (
	"context"

	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/configcenter"
)

func (s *ConfigCenter) NotifyListenersReloadConfig(ctx context.Context, req *configcenter.NotifyListenersReloadConfigReq) (*configcenter.NotifyListenersReloadConfigResp, error) {
	var resp configcenter.NotifyListenersReloadConfigResp

	if len(req.CollNames) == 0 && !req.ShouldReloadAll {
		return &resp, nil
	}

	if err := s.PubMulConfigChangedEvt(req.ShouldReloadAll, req.CollNames, req.ListenerGroup); err != nil {
		fastlog.Errorf("publish config changed event failed, err:%v", err)
		return nil, err
	}

	return &resp, nil
}
