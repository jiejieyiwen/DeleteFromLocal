package main

import (
	"Config"
	"DeleteFromLocal1/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"net/http"
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

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":9527", nil)
		if err != nil {
			logger.Errorf("http Listen Fail Err: [%v]", err)
			panic(err)
		}
	}()

	err := server.GetServerStream().InitServerStream()
	if err != nil {
		logger.Errorf("Init DFL Modular Fail Err: [%v]", err)
		return
	}
}
