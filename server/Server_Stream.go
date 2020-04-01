package server

import (
	"DeleteFromLocal1/MqModular"
	cli "StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"iPublic/EnvLoad"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net"
	"os"
	"sync"
	"time"
)

type ServerStream struct {
	m_mapMountPointLock sync.Mutex
	m_mapMountPoint     map[string]int
	m_strIP             string
	m_nPort             int
	m_RedisCon          *RedisModular.RedisConn
	m_strRedisUrl       string
	m_plogger           *logrus.Entry
	cli.UnimplementedGreeterServer
}

var serverstream ServerStream

func GetServerStream() *ServerStream {
	return &serverstream
}

var pMQConnectPool MqModular.ConnPool //MQ连接
var StrMQURL string                   //MQ连接地址
var chMQMsg chan cli.StreamResData

func (pThis *ServerStream) GetStream(req *cli.StreamReqData, srv cli.Greeter_GetStreamServer) error {
	if req == nil {
		pThis.m_plogger.Info("Empty Msg！~")
		return errors.New("Empty Msg")
	}

	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	pThis.m_plogger.Infof("Msg Recived~~~！:[%v]", req)

	if req.StrMountPoint == "" {
		pThis.m_plogger.Error("无挂载点~！")
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return errors.New("无挂载点")
	}
	if req.StrChannelID == "" {
		pThis.m_plogger.Error("无设备ID~！")
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return errors.New("无设备ID")
	}
	if len(req.StrChannelID) < 19 {
		pThis.m_plogger.Error("设备ID长度错误~！")
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return errors.New("设备ID长度错误")
	}
	if req.StrDate == "" {
		pThis.m_plogger.Error("无日期~！")
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return errors.New("无日期")
	}
	_, err := time.Parse("2006-01-02", req.StrDate)
	if err != nil {
		pThis.m_plogger.Error("日期格式错误~！")
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return errors.New("日期格式错误")
	}

	//拼凑需删除的文件夹路径
	path := req.StrMountPoint
	path += req.StrChannelID
	path += "/"
	path += req.StrDate
	pThis.m_plogger.Infof("Path is :[%v]", path)

	//判断路径是否存在
	_, err = os.Stat(path)
	if err != nil {
		pThis.m_plogger.Infof("该设备[%v]文件夹不存在~！", path)
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate})
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate}
		return err
	}

	//计算删除耗时
	t1 := time.Now()
	//脚本删除
	res, err := exec_shell(path)
	if err != nil {
		pThis.m_plogger.Errorf("删除文件出错：[%v]", err)
		srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID, StrDate: req.StrDate})
		return err
	}
	t2 := time.Now()
	pThis.m_plogger.Infof("Spend time:[%v] 毫秒", t2.Sub(t1).Milliseconds())
	pThis.m_plogger.Infof("Delete File Success, mountpoint is: [%v], RelativePath is: [%v], loacation is: [%v], RecordID is: [%v], ChannelID is [%v]~~!", req.StrMountPoint, req.StrRelativePath, res, req.StrRecordID, req.StrChannelID)

	//回复成功消息
	err = srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, NStartTime: req.NStartTime, StrMountPoint: req.StrMountPoint})
	if err != nil {
		pThis.m_plogger.Errorf("回复删除成功消息失败:[%v]", err)
		return err
	} else {
		pThis.m_plogger.Infof("回复删除成功消息成功~！")
	}
	return nil
}

func goSendMQMsg() {
	logger := LoggerModular.GetLogger()
	for task := range chMQMsg {
		sendmsg, err := json.Marshal(task)
		if err != nil {
			logger.Errorf("Mashal Failed:[%v]", err)
		}
		err = pMQConnectPool.SendWithQueue("RecordDelete", false, false, sendmsg, true)
		if err != nil {
			logger.Errorf("Publish Failed: [%v]", err)
		} else {
			logger.Info("Publish Success~!")
		}
	}
}

func (pThis *ServerStream) InitServerStream() error {
	pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	//获取挂载点
	//go pThis.GetMountPoint()

	//链接mq
	conf := EnvLoad.GetConf()
	conf.ServerConfig.RabbitURL = "http://192.168.0.56:8000/imccp-mediacore"
	StrMQURL = conf.ServerConfig.RabbitURL
	StrMQURL, size, err1 := MqModular.GetServerURL(StrMQURL)
	if err1 != nil {
		pThis.m_plogger.Errorf("Get MQ url form ConfigCenter Error:[%v]", err1)
		return err1
	}
	size = 1
	pMQConnectPool.Init(size, StrMQURL)

	chMQMsg = make(chan cli.StreamResData, 1024)

	go goSendMQMsg()

	//获取本机IP地址
	ip, err := pThis.GetIPAddres()
	if err != nil {
		pThis.m_plogger.Errorf("Get IP Address Err: [%v]", err)
		return err
	}

	//建立grpc监听
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		pThis.m_plogger.Errorf("Start GRPC Listen Err: [%v]", err)
		return err
	}
	pThis.m_plogger.Info("Start GRPC Listen Success")

	//获取端口
	port := lis.Addr().(*net.TCPAddr).Port

	//写入redis
	err = pThis.WriteToRedis(ip, port)
	if err != nil {
		pThis.m_plogger.Errorf("Write Data to Redis Err: [%v]", err)
		return err
	}
	pThis.m_plogger.Info("Write Data to Redis Success")

	//grpc stream服务
	s := grpc.NewServer()
	//注册事件
	cli.RegisterGreeterServer(s, &ServerStream{})
	//处理链接
	err = s.Serve(lis)
	if err != nil {
		pThis.m_plogger.Errorf("Process Link Err: [%v]", err)
		return err
	}
	return nil
}
