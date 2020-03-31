package server

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net"
	"strconv"
	"strings"
	"time"
)

//获取ip地址
func (pThis *ServerStream) GetIPAddres() (add string, err error) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			addrs, _ := netInterfaces[i].Addrs()
			for _, address := range addrs {
				if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						//172.17 172.16
						if strings.Contains(ipnet.IP.String(), "192.168") {
							return ipnet.IP.String(), nil
						}
						//Plogger.Infof("Ip is [%v]", ipnet.IP.String())
					}
				}
			}
		}
	}
	return "", errors.New("No IP Address Get")
}

//写入redis
func (pThis *ServerStream) WriteToRedis(ip string, port int) (err error) {
	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	//链接redis
	conf := EnvLoad.GetConf()
	pThis.m_RedisCon = RedisModular.GetRedisPool()
	pThis.m_strRedisUrl = conf.ServerConfig.RedisURL

	//pThis.m_strRedisUrl = "redis://:S0o9l@7&PO@49.234.88.77:8888/8"
	//pThis.m_strRedisUrl = "redis://:B9OxgC3HYg@192.168.0.56:30003/6"
	//pThis.m_strRedisUrl = "redis://:inphase123.@127.0.0.1:15675/2"

	err = pThis.m_RedisCon.DaliWithURL(pThis.m_strRedisUrl)
	if err != nil {
		pThis.m_plogger.Errorf("Init Redis Failed, URL:%v, errors: %v", pThis.m_strRedisUrl, err)
		return err
	}
	pThis.m_plogger.Infof("Init Redis Success~!", pThis.m_strRedisUrl)

	//判断ip和port是否存在
	if ip == "" || port == 0 {
		return errors.New("No IP or Port~~!")
	}
	pThis.m_strIP = ip
	pThis.m_nPort = port

	//IP地址、挂载点写入
	value := ip + ":" + strconv.Itoa(port)
	temmap := pThis.GetMountPointMap()
	var value1 string
	for key, _ := range temmap {
		value1 += key
		value1 += ":"
	}
	go func() {
		for {
			StatusCmd := pThis.m_RedisCon.Client.Set("DeleteServer:"+value, value1, time.Second*60*25)
			if StatusCmd.Err() != nil {
				pThis.m_plogger.Errorf("Write MountPoint to Redis Falied:[%v]", StatusCmd.Err())
			} else {
				pThis.m_plogger.Info("Write MountPoint to Redis Success")
				time.Sleep(time.Second * 60 * 25)
			}
		}
	}()

	return nil
}
