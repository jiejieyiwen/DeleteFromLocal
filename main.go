package main

import (
	"DeleteFromLocal1/Config"
	"DeleteFromLocal1/server"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
)

func init() {
	EnvLoad.GetCmdLineConfig()
}

func main() {
	logger := LoggerModular.GetLogger()

	//conf := EnvLoad.GetConf()
	//if err := conf.InitConfig(); err != nil {
	//	logger.Error(err)
	//	return
	//}

	config := Config.GetConfig()
	if err := Config.ReadConfig(); err != nil {
		logger.Error(err)
		return
	}
	logger.Info(config)

	err := server.GetServerStream().InitServerStream()
	if err != nil {
		logger.Errorf("Init DFL Modular Fail Err: [%v]", err)
		return
	}
}
