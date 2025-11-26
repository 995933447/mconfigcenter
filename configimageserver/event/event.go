package event

import (
	"github.com/995933447/mconfigcenter/configcenter"
	"github.com/995933447/mconfigcenter/configimageserver/config"
	"github.com/995933447/natsevent"
)

func RegisterEventListeners() error {
	var subOpts []natsevent.ApplySubOptsFunc
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		subOpts = append(subOpts, natsevent.WithConnName(c.GetSubNotificationNatsConn()))
		if maxWait := c.GetSubNotificationNatsMaxAckWait(); maxWait > 0 {
			subOpts = append(subOpts, natsevent.WithMaxAckWait(maxWait))
		}
		if heart := c.GetSubNotificationNatsIdleHeartbeat(); heart > 0 {
			subOpts = append(subOpts, natsevent.WithIdleHeartbeat(heart))
		}
	})
	err := natsevent.Subscribe(
		configcenter.EventNameConfigChangedOnlyForImage,
		configcenter.EasymicroGRPCPbServiceNameConfigCenter,
		OnConfigChangedEvent,
		subOpts...,
	)
	if err != nil {
		return err
	}
	return nil
}
