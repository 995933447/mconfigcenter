package handler

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/mconfigcenter/common"
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configcenterserver/config"
	"github.com/995933447/mgorm"
	"github.com/995933447/natsevent"
	"github.com/xeipuuv/gojsonschema"
)

type ConfigCenter struct {
	configcenter.UnimplementedConfigCenterServer
	ServiceName string
}

var ConfigCenterHandler = &ConfigCenter{
	ServiceName: configcenter.EasymicroGRPCPbServiceNameConfigCenter,
}

func (s *ConfigCenter) newKVConfigModel() *common.KVConfigModel {
	mod := common.NewKVConfigModel()
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		mod.SetConn(c.GetMongoConn())
		mod.SetDb(c.GetMongoDb())
	})
	return mod
}

func (s *ConfigCenter) newConfigSchemaModel() *configcenter.ConfigSchemaModel {
	mod := configcenter.NewConfigSchemaModel()
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		mod.SetConn(c.GetMongoConn())
		mod.SetDb(c.GetMongoDb())
	})
	return mod
}

func (s *ConfigCenter) newGeneralModel(collName string) *mgorm.Orm {
	var conn, db string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		conn = c.GetMongoConn()
		db = c.GetMongoDb()
	})

	return mgorm.NewOrm(
		conn,
		db,
		collName,
		false,
		nil,
		nil,
		nil,
		nil,
	)
}

func (s *ConfigCenter) validateSchema(ctx context.Context, collName string, data any) error {
	getSchemaResp, err := s.GetConfigSchema(ctx, &configcenter.GetConfigSchemaReq{
		CollName: collName,
	})
	if err != nil {
		if grpc.IsRPCErr(err, configcenter.ErrCode_ErrCodeConfigSchemaNotFound.Number()) {
			return nil
		}

		fastlog.Error(err.Error())
		return err
	}

	if getSchemaResp.Schema.JsonSchema == "" {
		return nil
	}

	schemaChecker := gojsonschema.NewStringLoader(getSchemaResp.Schema.JsonSchema)
	docLoader := gojsonschema.NewGoLoader(data)
	result, err := gojsonschema.Validate(schemaChecker, docLoader)
	if err != nil {
		return err
	}

	if !result.Valid() {
		var b strings.Builder
		for _, resErr := range result.Errors() {
			line := resErr.String()
			// 过滤：如果 Expected 是 array/object/map，则忽略这条错误
			if s.shouldIgnoreInvalidTypeForValidateSchemaErr(line) {
				continue
			}
			b.WriteString(line)
			b.WriteByte('\n')
		}
		errMsg := strings.TrimSpace(b.String())
		if errMsg != "" {
			return errors.New(errMsg)
		}
	}

	return nil
}

// 预编译正则：抓取 Expected 与 given 之间的“期望类型片段”
var reInvalidTypeExpectedForValidateSchemaErr = regexp.MustCompile(`(?mi)Invalid type\.\s*Expected:\s*([^,]+)\s*,\s*given:\s*null\b`)

// 返回 true 表示应该忽略（放行）的错误：Expected 包含 array/object/map
func (s *ConfigCenter) shouldIgnoreInvalidTypeForValidateSchemaErr(line string) bool {
	m := reInvalidTypeExpectedForValidateSchemaErr.FindStringSubmatch(line)
	if len(m) < 2 {
		return false
	}
	exp := strings.ToLower(strings.TrimSpace(m[1]))
	// JSON Schema 标准里只会出现 array/object；map 只是额外兜底（防自定义文案）
	return strings.Contains(exp, "array") || strings.Contains(exp, "object") || strings.Contains(exp, "map")
}

func (s *ConfigCenter) pubConfigChangedEvt(collName, listenerGroup string, changedConfigIds ...string) error {
	var err error
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if c.ListenerNotificationDirectness == config.ListenerNotificationDirectnessThroughImageServer {
			err = (&configcenter.ConfigChangedEvent{
				RefreshListenerGroup: listenerGroup,
				Configs: []*configcenter.ConfigChangedEventConfig{
					{
						ConfigIds: changedConfigIds,
						CollName:  collName,
					},
				},
			}).SendToImages(natsevent.WithPubConnName(c.GetPubNotificationNatsConn()))
			if err != nil {
				fastlog.Error(err.Error())
				return
			}
			return
		}

		err = (&configcenter.ConfigChangedEvent{
			RefreshListenerGroup: listenerGroup,
			Configs: []*configcenter.ConfigChangedEventConfig{
				{
					ConfigIds: changedConfigIds,
					CollName:  collName,
				},
			},
		}).Send(natsevent.WithPubConnName(c.GetPubNotificationNatsConn()))
		if err != nil {
			fastlog.Error(err.Error())
			return
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *ConfigCenter) PubMulConfigChangedEvt(shouldReloadAll bool, collNames []string, listenerGroup string) error {
	var configs []*configcenter.ConfigChangedEventConfig
	if !shouldReloadAll {
		for _, collName := range collNames {
			configs = append(configs, &configcenter.ConfigChangedEventConfig{
				CollName: collName,
			})
		}
	}

	var err error
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if c.ListenerNotificationDirectness == config.ListenerNotificationDirectnessThroughImageServer {
			err = (&configcenter.ConfigChangedEvent{
				RefreshListenerGroup: listenerGroup,
				Configs:              configs,
				ShouldReloadAll:      shouldReloadAll,
			}).SendToImages(natsevent.WithPubConnName(c.GetPubNotificationNatsConn()))
			if err != nil {
				fastlog.Error(err.Error())
				return
			}
			return
		}

		err = (&configcenter.ConfigChangedEvent{
			RefreshListenerGroup: listenerGroup,
			Configs:              configs,
			ShouldReloadAll:      shouldReloadAll,
		}).Send(natsevent.WithPubConnName(c.GetPubNotificationNatsConn()))
		if err != nil {
			fastlog.Error(err.Error())
			return
		}
	})
	if err != nil {
		return err
	}

	return nil
}
