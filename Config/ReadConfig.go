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
	DataUrlConfig DataUrlConfig
}

type StorageConfig struct {
	ConcurrentNumber int `yml:"ConcurrentNumber" json:"ConcurrentNumber"`
}

type PublicConfig struct {
	RedisURL   string `yml:"RedisURL" json:"RedisURL"`
	AMQPURL    string `yml:"AMQPURL" json:"AMQPURL"`
	MongoDBURL string `yml:"MongoDBURL" json:"MongoDBURL"`
}

type DataUrlConfig struct {
	DataURL string `yml:"DataURL" json:"DataURL"`
	AuthURL string `yml:"AuthURL" json:"AuthURL"`
}

func ReadConfig() error {
	c := EnvLoad.GetConf()
	logger := LoggerModular.GetLogger().WithFields(logrus.Fields{
		"MediaConfig": c.MediaConfig, "Priority": c.Priority, "PriorityLimit": c.PriorityLimit,
	})
	if Config, err := iConfig.GetConfigInterface(1); err != nil {
		return err
	} else {
		err := Config.ReadConfig(c.MediaConfig, "/imccp-mediacore-storage/RecordDeleteService", &config.StorageConfig)
		if err != nil {
			logger.Errorf("Get RecordDeleteService.yml err: %s", err.Error())
			return err
		}
		err = Config.ReadConfig(c.MediaConfig, "/imccp-mediacore-public/Config", &config.PublicConfig)
		if err != nil {
			logger.Errorf("Get public Config.yml err: %s", err.Error())
			return err
		}
		err = Config.ReadConfig(c.MediaConfig, "/imccp-mediacore-public/DataURL", &config.DataUrlConfig)
		if err != nil {
			logger.Errorf("Get DataUrl Config.yml err: %s", err.Error())
			return err
		}
	}
	return nil
}
