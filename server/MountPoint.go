package server

import (
	"github.com/Terry-Mao/goim/pkg/bufio"
	"github.com/sirupsen/logrus"
	"iPublic/LoggerModular"
	"os/exec"
	"strings"
	"time"
)

func (pThis *ServerStream) GetMountPoint() {
	/*
	   定时去扫描Linux系统下的挂载点  60s
	*/
	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	for {
		pThis.m_mapMountPointLock.Lock()
		pThis.m_mapMountPoint = make(map[string]int)
		cmd := exec.Command("/bin/bash", "-c", `df -h`)
		//创建获取命令输出管道
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			pThis.m_plogger.Errorf("Error:can not obtain stdout pipe for command: [%v]", err)
			return
		}
		//执行命令
		if err := cmd.Start(); err != nil {
			pThis.m_plogger.Errorf("Error:The command is err: [%v]", err)
			return
		}
		//使用带缓冲的读取器
		outputBuf := bufio.NewReader(stdout)
		bFirst := true
		for {
			//一次获取一行,_ 获取当前行是否被读完
			output, _, err := outputBuf.ReadLine()
			if err != nil {

				// 判断是否到文件的结尾了否则出错
				if err.Error() != "EOF" {
					pThis.m_plogger.Errorf("EOF,Error: [%v]", err)
				} else {
					break
				}
			}
			strLineData := string(output)
			if bFirst {
				bFirst = false
				continue
			}
			sliceStr := strings.Split(strLineData, "% ")
			sliceStr[1] += "/"
			pThis.m_mapMountPoint[sliceStr[1]] = 1
		}
		pThis.m_mapMountPointLock.Unlock()
		pThis.m_plogger.Infof("Update MountPoint Compelete")
		time.Sleep(time.Second * 60 * 30)
	}
}

func (pThis *ServerStream) GetMountPointByShell() {
	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	pThis.m_mapMountPointLock.Lock()
	pThis.m_mapMountPoint = make(map[string]int)
	strLineData, err := getMountPoint()
	if err != nil {
		pThis.m_plogger.Errorf("获取挂载点出错: [%v]", err)
	} else {
		//pThis.m_plogger.Infof("挂载点为: [%v]", strLineData)
		sliceStr := strings.Split(strLineData, "\n")
		for _, s := range sliceStr {
			s += "/"
			pThis.m_mapMountPoint[s] = 1
		}
		pThis.m_mapMountPointLock.Unlock()
		pThis.m_plogger.Infof("Update MountPoint Compelete")
	}
}

//获取保存好的挂载点
func (pThis *ServerStream) GetMountPointMap() map[string]int {
	pThis.m_mapMountPointLock.Lock()
	mountpoin := pThis.m_mapMountPoint
	pThis.m_mapMountPointLock.Unlock()
	return mountpoin
}
