package rpctest

import (
	"testing"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/mconfigcenter/common"
	"github.com/995933447/runtimeutil"
	"go.mongodb.org/mongo-driver/bson"

	"context"
	"log"

	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configcenterserver/boot"
	"github.com/995933447/mconfigcenter/configcenterserver/config"
	"github.com/995933447/mconfigcenter/configcenterserver/event"
)

func TestConfigCenterHdlDeleteConfig(t *testing.T) {
	InitEnv()

	_, err := configcenter.ConfigCenterGRPC().DeleteConfig(context.Background(), &configcenter.DeleteConfigReq{
		CollName:              "kv_config",
		Id:                    "691257d5500d45ff4c71919d",
		ShouldNotifyListeners: true,
	})
	if err != nil {
		log.Printf("config delete config err:%v", err)
	}
}

func TestConfigCenterHdlUpdateConfigs(t *testing.T) {
	InitEnv()

	data := &common.KVConfig{
		Key:   "foo",
		Value: "barbarbar",
	}

	b, err := bson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := configcenter.ConfigCenterGRPC().UpdateConfigs(context.Background(), &configcenter.UpdateConfigsReq{
		Ids:                   []string{"691257d5500d45ff4c71919d", "6912cae3500d45ff4c7191a3", "6912caea500d45ff4c7191a4"},
		CollName:              "kv_config",
		Value:                 b,
		ShouldNotifyListeners: true,
		ListenerGroup:         "SCRM",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(resp)
}

func TestConfigCenterHdlUpdateConfig(t *testing.T) {
	InitEnv()

	data := &common.KVConfig{
		Key:   "foo",
		Value: "barbarbar",
	}

	b, err := bson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := configcenter.ConfigCenterGRPC().UpdateConfig(context.Background(), &configcenter.UpdateConfigReq{
		Id:                    "691257d5500d45ff4c71919d",
		CollName:              "kv_config",
		Value:                 b,
		ShouldNotifyListeners: true,
		ListenerGroup:         "SCRM",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(resp)
}

func TestConfigCenterHdlAddConfigs(t *testing.T) {
	InitEnv()

	var values [][]byte
	data := &common.KVConfig{
		Key:   "foo30",
		Value: "bar30",
	}

	b, err := bson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	values = append(values, b)

	data = &common.KVConfig{
		Key:   "foo31",
		Value: "bar31",
	}

	b, err = bson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	values = append(values, b)

	data = &common.KVConfig{
		Key:   "foo32",
		Value: "bar32",
	}

	b, err = bson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	values = append(values, b)

	resp, err := configcenter.ConfigCenterGRPC().AddConfigs(context.Background(), &configcenter.AddConfigsReq{
		CollName:              "kv_config",
		Values:                values,
		ShouldNotifyListeners: true,
		ListenerGroup:         "SCRM",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(resp)
}

func TestConfigCenterHdlAddConfig(t *testing.T) {
	InitEnv()

	data := &common.KVConfig{
		Key:   "foo",
		Value: "bar",
	}

	b, err := bson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := configcenter.ConfigCenterGRPC().AddConfig(context.Background(), &configcenter.AddConfigReq{
		CollName:              "kv_config",
		Value:                 b,
		ShouldNotifyListeners: true,
		ListenerGroup:         "SCRM",
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(resp)
}

func InitEnv() {
	if err := boot.InitNode("configcenter"); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := config.LoadConfig(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := boot.InitMgorm(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := event.RegisterEventListeners(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if !c.IsProd() {
			if err := boot.RegisterNatsRPCRoutes(); err != nil {
				log.Fatal(err)
			}
		}
	})

	if err := grpc.PrepareDiscoverGRPC(context.TODO(), configcenter.EasymicroGRPCSchema, configcenter.EasymicroDiscoveryName); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}
}
