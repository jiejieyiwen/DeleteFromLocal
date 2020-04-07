package server

import (
	"Config"
	"DeleteFromLocal1/MqModular"
	cli "StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
var chTask chan *cli.StreamReqData

func goDeleteFile(req *cli.StreamReqData) {
	logger := LoggerModular.GetLogger()
	if req.StrMountPoint == "" {
		logger.Error("无挂载点~！")
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	if req.StrChannelID == "" {
		logger.Error("无设备ID~！")
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	if len(req.StrChannelID) < 19 {
		logger.Error("设备ID长度错误~！")
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	if req.StrDate == "" {
		logger.Error("无日期~！")
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return

	}
	_, err := time.Parse("2006-01-02", req.StrDate)
	if err != nil {
		logger.Error("日期格式错误~！")
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}

	//拼凑需删除的文件夹路径
	path := req.StrMountPoint
	path += req.StrChannelID
	path += "/"
	path += req.StrDate
	logger.Infof("Path is :[%v]", path)

	//判断路径是否存在
	_, err = os.Stat(path)
	if err != nil {
		logger.Infof("该设备[%v]文件夹不存在~！", path)
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}

	//计算删除耗时
	t1 := time.Now()
	//脚本删除
	res, err := exec_shell(path)
	if err != nil {
		logger.Errorf("删除文件出错：[%v]", err)
		chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	t2 := time.Now()
	logger.Infof("Spend time:[%v] 毫秒", t2.Sub(t1).Milliseconds())
	logger.Infof("Delete File Success, mountpoint is: [%v], RelativePath is: [%v], loacation is: [%v], RecordID is: [%v], ChannelID is [%v]~~!", req.StrMountPoint, req.StrRelativePath, res, req.StrRecordID, req.StrChannelID)
	chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
}

func goStartDelete() {
	for task := range chTask {
		go goDeleteFile(task)
	}
}

func (pThis *ServerStream) GetStream(req *cli.StreamReqData, srv cli.Greeter_GetStreamServer) error {
	if req == nil {
		pThis.m_plogger.Info("Empty Msg！~")
		return errors.New("Empty Msg")
	}
	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	pThis.m_plogger.Infof("Msg Recived~~~！:[%v]", req)
	srv.Send(&cli.StreamResData{NRespond: 1})
	chTask <- req
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
	go pThis.GetMountPoint()

	//链接mq
	//StrMQURL := Config.GetConfig().PublicConfig.AMQPURL
	////StrMQURL = "http://192.168.0.56:8000/imccp-mediacore"
	//size := 1
	//pMQConnectPool.Init(size, StrMQURL)
	//
	//go goSendMQMsg()

	chTask = make(chan *cli.StreamReqData, Config.GetConfig().StorageConfig.ConcurrentNumber)
	chMQMsg = make(chan cli.StreamResData, 1024)

	go goStartDelete()

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
