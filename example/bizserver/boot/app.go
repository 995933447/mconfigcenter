package boot

import (
	"log"

	"github.com/995933447/mconfigcenter/example/bizserver/config"
	"github.com/995933447/mconfigcenter/example/bizserver/confighub"
	"github.com/995933447/runtimeutil"
)

func InitApp() {
	var listenerGroup string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		listenerGroup = c.ListenerGroup
	})
	err := confighub.LoadConfigHub(listenerGroup)
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}
}
