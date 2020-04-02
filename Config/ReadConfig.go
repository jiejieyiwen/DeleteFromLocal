package Config

import (
	"github.com/sirupsen/logrus"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/iConfig"
)

var config Config

//http://config.mj.cn/imccp-mediacore
func GetConfig() *Config {
	return &config
}

type Config struct {
	StorageConfig StorageConfig
	PublicConfig  PublicConfig
}

type StorageConfig struct {
	ConcurrentNumber int `yaml:"ConcurrentNumber" json:"ConcurrentNumber"`
}

type PublicConfig struct {
	RedisUrl string `yaml:"RedisURL" json:"RedisURL"`
	AMQPURL  string `yaml:"AMQPURL" json:"AMQPURL"`
}

func ReadConfig() error {
	c := EnvLoad.GetConf()
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{
		"MediaConfig": c.MediaConfig, "Priority": c.Priority, "PriorityLimit": c.PriorityLimit,
	})
	if Config, err := iConfig.GetConfigInterface(1); err != nil {
		return err
	} else {
		err := Config.ReadConfig(c.MediaConfig, "-storage/RecordDeleteService", &config.StorageConfig)
		if err != nil {
			logger.Errorf("Get RecordDeleteService.yaml err: %s", err.Error())
			return err
		}
		err = Config.ReadConfig(c.MediaConfig, "-public-Config", &config.PublicConfig)
		if err != nil {
			logger.Errorf("Get public/Config.yaml err: %s", err.Error())
			return err
		}
	}
	return nil
}
