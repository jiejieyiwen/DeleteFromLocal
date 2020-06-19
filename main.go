package main

import (
	"Config"
	Mongomon "DeleteFromLocal/Mongo"
	"DeleteFromLocal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net/http"
	"os"
	"strconv"
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

	if len(os.Args) > 1 {
		for index, k := range os.Args {
			switch k {
			case "-Con":
				{
					server.ConcurrentNumber, _ = strconv.Atoi(os.Args[index+1])
				}
			}
		}
	}

	logger.Infof("ConcurrentNumber: [%v]", server.ConcurrentNumber)

	if err := Mongomon.GetMongoManager().Init(); err != nil {
		logger.Errorf("Init Mongo Fail Err: [%v]", err)
		panic(err)
		return
	}

	c := cron.New()
	_, err := c.AddFunc("00 6 * * *", server.GetServerStream().GetFailedFile)
	if err != nil {
		logger.Error(err)
		return
	}
	c.Start()
	defer c.Stop()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":41011", nil)
		if err != nil {
			logger.Errorf("http Listen Fail Err: [%v]", err)
			panic(err)
			return
		}
	}()

	conf := EnvLoad.GetConf()
	conf.HttpPort = 41011
	conf.RedisAppName = "imccp-mediacore-media-DeleteFromLocal"
	RedisModular.GetBusinessMap().SetBusinessRedis(EnvLoad.PublicName, Config.GetConfig().PublicConfig.RedisURL)
	EnvLoad.GetServiceManager().SetStatus(EnvLoad.ServiceStatusOK)
	go EnvLoad.GetServiceManager().RegSelf()

	err = server.GetServerStream().InitServerStream()
	if err != nil {
		logger.Errorf("Init DFL Modular Fail Err: [%v]", err)
		return
	}
}
