package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/995933447/easymicro/elect"
	"github.com/995933447/easymicro/grpc/interceptor"
	"github.com/995933447/mconfigcenter/configimage"
	"github.com/995933447/mconfigcenter/configimageserver/boot"
	"github.com/995933447/mconfigcenter/configimageserver/config"
	"github.com/995933447/mconfigcenter/configimageserver/event"
	ggrpc "google.golang.org/grpc"

	"github.com/995933447/discovery"
	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/runtimeutil"
)

func main() {
	if err := boot.InitNode("configimage"); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := config.LoadConfig(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	boot.InitRouteredis()

	if err := boot.InitElect(context.TODO()); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := boot.InitMgorm(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if ok, err := elect.IsMaster(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	} else if ok {
		if err := event.RegisterEventListeners(); err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}
	} else {
		go func() {
			for {
				if ok, err := elect.IsMaster(); err != nil {
					log.Fatal(runtimeutil.NewStackErr(err))
				} else if ok {
					if err := event.RegisterEventListeners(); err != nil {
						log.Fatal(runtimeutil.NewStackErr(err))
					}
					break
				}

				time.Sleep(time.Second * 3)
			}
		}()
	}

	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if !c.IsProd() {
			if err := boot.RegisterNatsRPCRoutes(); err != nil {
				log.Fatal(runtimeutil.NewStackErr(err))
			}
		}
	})

	if err := grpc.PrepareDiscoverGRPC(context.TODO(), configimage.EasymicroGRPCSchema, configimage.EasymicroDiscoveryName); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	boot.RegisterGRPCDialOpts()

	signal, err := boot.InitSignal()
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())
	gracefulStopCtx, gracefulStopCancel := context.WithCancel(stopCtx)

	err = signal.AppendSignalCallbackByAlias(boot.SignalAliasStop, func() {
		gracefulStopCancel()
	})
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	err = signal.AppendSignalCallbackByAlias(boot.SignalAliasInterrupt, func() {
		stopCancel()
	})
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	err = grpc.ServeGRPC(context.TODO(), &grpc.ServeGRPCOptions{
		DiscoveryName:   configimage.EasymicroDiscoveryName,
		ServiceNames:    boot.ServiceNames,
		StopCtx:         stopCtx,
		GracefulStopCtx: gracefulStopCtx,
		OnRunServer: func(server *ggrpc.Server, node *discovery.Node) {
			signal.Start()
			boot.InitApp()

			log.Printf("up node %s:%d !\n", node.Host, node.Port)
			log.Printf(">>>>>>>>>>>>>>> run %s successfully ! <<<<<<<<<<<<<<<", strings.Join(boot.ServiceNames, ", "))
		},
		RegisterServiceServersFunc: boot.RegisterServiceServers,
		EnabledHealth:              true,
		GRPCServerOpts: []ggrpc.ServerOption{
			ggrpc.ChainUnaryInterceptor(
				interceptor.TraceServeRPCUnaryInterceptor,
				interceptor.FastlogServeRPCUnaryInterceptor,
				interceptor.RecoveryServeRPCUnaryInterceptor,
			),
			ggrpc.ChainStreamInterceptor(
				interceptor.TraceServeRPCStreamInterceptor,
				interceptor.FastlogServeRPCStreamInterceptor,
				interceptor.RecoveryServeRPCStreamInterceptor,
			),
		},
	})
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}
}
