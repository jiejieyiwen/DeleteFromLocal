package main

import (
	svr "DeleteFromLocal1/server"
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

	err := svr.GetServerStream().InitServerStream()
	if err != nil {
		logger.Errorf("Init DFL Modular Fail Err: [%v]", err)
		return
	}
}
