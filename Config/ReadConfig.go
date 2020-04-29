package Config

import (
	"github.com/sirupsen/logrus"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/iConfig"
)

var config Config

func GetConfig() *Config {
	return &config
}

type Config struct {
	StorageTs StoragTSeDelete
}
type StoragTSeDelete struct {
	ConcurrentNumber int `yaml:"ConcurrentNumber" json:"ConcurrentNumber"`
}

func ReadConfig() error {
	c := EnvLoad.GetConf()
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{
		"MediaConfig": c.MediaConfig, "Priority": c.Priority, "PriorityLimit": c.PriorityLimit,
	})
	if Config, err := iConfig.GetConfigInterface(0); err != nil {
		return err
	} else {
		err := Config.ReadConfig(c.MediaConfig, "/imccp-mediacore-storage/Config", &config.StorageTs)
		if err != nil {
			logger.Errorf("Get imccp-mediacore-storage/Config err: %s", err.Error())
			return err
		}
	}
	return nil
}
