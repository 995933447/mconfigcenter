package configimage

import (
	"context"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"google.golang.org/grpc"
)

func PrepareGRPC(discoveryName string, dialGRPCOpts ...grpc.DialOption) error {
	if discoveryName == "" {
		discoveryName = EasymicroDiscoveryName
	}

	if err := easymicrogrpc.PrepareDiscoverGRPC(context.TODO(), EasymicroGRPCSchema, discoveryName); err != nil {
		return err
	}

	easymicrogrpc.RegisterServiceDialOpts(EasymicroGRPCPbServiceNameConfigImage, true, dialGRPCOpts...)

	return nil
}
