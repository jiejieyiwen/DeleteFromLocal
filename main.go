package main

import (
	"Config"
	"DeleteFromLocal1/server"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
)

func init() {
	EnvLoad.GetCmdLineConfig()
}

func main() {
	logger := LoggerModular.GetLogger()

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Infof("config is: [%v]", config)

	err := server.GetServerStream().InitServerStream()
	if err != nil {
		logger.Errorf("Init DFL Modular Fail Err: [%v]", err)
		return
	}
}
