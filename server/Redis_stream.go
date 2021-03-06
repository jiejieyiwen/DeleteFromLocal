package server

import (
	"Config"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
func (pThis *ServerStream) Initedis() error {
	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	//链接存储redis
	pThis.StorageRedis = RedisModular.GetRedisPool()
	pThis.StorageRedisUrl = Config.GetConfig().StorageRedis.RedisURL

	//pThis.m_strRedisUrl = "redis://:S0o9l@7&PO@49.234.88.77:8888/8"
	//pThis.m_strRedisUrl = "redis://:B9OxgC3HYg@192.168.0.56:30003/6"
	//pThis.StorageRedisUrl = "redis://:inphase123.@127.0.0.1:15679/2"
	//pThis.m_strRedisUrl = "redis://:inphase123.@192.168.2.64:23680/2"

	err := pThis.StorageRedis.DaliWithURL(pThis.StorageRedisUrl)
	if err != nil {
		pThis.m_plogger.Errorf("Init StorageRedis Failed, URL:%v, errors: %v", pThis.StorageRedisUrl, err)
		return err
	}
	pThis.m_plogger.Infof("Init StorageRedis Success~!", pThis.StorageRedisUrl)

	//链接redis
	pThis.m_RedisCon = RedisModular.GetRedisPool()
	pThis.m_strRedisUrl = Config.GetConfig().PublicConfig.RedisURL

	//pThis.m_strRedisUrl = "redis://:S0o9l@7&PO@49.234.88.77:8888/8"
	//pThis.m_strRedisUrl = "redis://:B9OxgC3HYg@192.168.0.56:30003/6"
	//pThis.m_strRedisUrl = "redis://:inphase123.@127.0.0.1:15679/2"
	//pThis.m_strRedisUrl = "redis://:inphase123.@192.168.2.64:23680/2"

	err = pThis.m_RedisCon.DaliWithURL(pThis.m_strRedisUrl)
	if err != nil {
		pThis.m_plogger.Errorf("Init Redis Failed, URL:%v, errors: %v", pThis.m_strRedisUrl, err)
		return err
	}
	pThis.m_plogger.Infof("Init Redis Success~!", pThis.m_strRedisUrl)
	pThis.MountPonitTask = make(map[string][]*StreamReqData)

	temmap := pThis.GetMountPointMap()
	pThis.MountPonitTaskLock.Lock()
	for key, _ := range temmap {
		pThis.MountPonitTask[key] = []*StreamReqData{}
		go pThis.goDeleteFileByMountPoint(key)
	}
	pThis.MountPonitTaskLock.Unlock()
	return nil
}

func (pThis *ServerStream) goUpdateMountPoint(ip string, port int) {
	//判断ip和port是否存在
	if ip == "" || port == 0 {
		pThis.m_plogger.Errorf("No IP or Port~~!")
		return
	}
	pThis.m_strIP = ip
	pThis.m_nPort = port
	value := ip
	mapHostManager := map[string]string{
		"192.168.2.131": "10.0.2.131",
		"192.168.2.132": "10.0.2.132",
		"192.168.2.133": "10.0.2.133",
		"192.168.2.134": "10.0.2.134",
		"192.168.2.79":  "10.0.2.79",
		"192.168.2.84":  "10.0.2.84",
		"192.168.2.50":  "192.168.2.50",
		"192.168.2.51":  "192.168.2.51",
		"192.168.2.52":  "192.168.2.52",
		"192.168.2.53":  "192.168.2.53",
		"192.168.0.122": "192.168.0.122",
		"192.168.0.101": "192.168.0.101",
	}
	for {
		//获取挂载点
		pThis.GetMountPointByShell()

		temmap := pThis.GetMountPointMap()
		var value1 string
		pThis.MountPonitTaskLock.Lock()
		for key, _ := range temmap {
			value1 += key
			value1 += ":"
			if _, ok := pThis.MountPonitTask[key]; !ok {
				pThis.MountPonitTask[key] = []*StreamReqData{}
				go pThis.goDeleteFileByMountPoint(key)
			}
		}
		pThis.MountPonitTaskLock.Unlock()

		StatusCmd := pThis.m_RedisCon.Client.Set("DeleteServer:"+value, value1, time.Second*60*5)
		if StatusCmd.Err() != nil {
			pThis.m_plogger.Errorf("Write MountPoint to Redis Falied:[%v]", StatusCmd.Err())
		} else {
			pThis.m_plogger.Info("Write MountPoint to Redis Success")
		}

		key := "Host_DeleteServerManager_"
		key += ip
		values := mapHostManager[ip]
		values += ":" + strconv.Itoa(port)

		StatusCmd = pThis.m_RedisCon.Client.Set(key, values, time.Second*60*5)
		if StatusCmd.Err() != nil {
			pThis.m_plogger.Errorf("Write Host_DeleteServerManager to Redis Falied:[%v]", StatusCmd.Err())
		} else {
			pThis.m_plogger.Info("Write Host_DeleteServerManager to Redis Success")

		}
		time.Sleep(time.Second * 60 * 5)
	}
}
