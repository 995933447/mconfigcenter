package configcenter

import (
	"github.com/995933447/natsevent"
)

const (
	EventNameConfigChangedOnlyForImage = "configcenter.ConfigChangedOnlyForImage"
	EventNameConfigChanged             = "configcenter.ConfigChanged"
)

type ConfigChangedEventConfig struct {
	CollName  string
	ConfigIds []string
}

type ConfigChangedEvent struct {
	Configs              []*ConfigChangedEventConfig `json:"configs"`
	RefreshListenerGroup string                      `json:"refresh_listener_group"`
	ShouldReloadAll      bool                        `json:"should_reload_all"`
}

func (e *ConfigChangedEvent) SendToImages(opts ...natsevent.ApplyPubOptsFunc) error {
	return natsevent.Publish(EventNameConfigChangedOnlyForImage, e, opts...)
}

func (e *ConfigChangedEvent) Send(opts ...natsevent.ApplyPubOptsFunc) error {
	return natsevent.Publish(EventNameConfigChanged, e, opts...)
}
