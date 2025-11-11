package handler

import (
	"github.com/995933447/mconfigcenter/example/biz"
)

type Biz struct {
	biz.UnimplementedBizServer
	ServiceName string
}

var BizHandler = &Biz{
	ServiceName: biz.EasymicroGRPCPbServiceNameBiz,
}