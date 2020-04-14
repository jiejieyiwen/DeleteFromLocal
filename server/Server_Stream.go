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
	MountPonitTask map[string]chan *cli.StreamReqData
	chMQMsg        chan cli.StreamResData
	pMQConnectPool MqModular.ConnPool //MQ连接
	StrMQURL       string             //MQ连接地址
}

var serverstream ServerStream

func GetServerStream() *ServerStream {
	return &serverstream
}

func (pThis *ServerStream) goDeleteFileByMountPoint(mp string, task chan *cli.StreamReqData) {
	logger := LoggerModular.GetLogger()
	logger.Infof("开启线程：[%v]", mp)
	for req := range task {
		go pThis.goDelete(mp, req)
	}
}

func (pThis *ServerStream) goDelete(mp string, req *cli.StreamReqData) {
	logger := LoggerModular.GetLogger()
	logger.Infof("线程：[%v] 处理任务：[%v]", mp, req)
	if req.StrMountPoint == "" {
		logger.Error("无挂载点~！")
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	if req.StrChannelID == "" {
		logger.Error("无设备ID~！")
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	if len(req.StrChannelID) < 19 {
		logger.Error("设备ID长度错误~！")
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	if req.StrDate == "" {
		logger.Error("无日期~！")
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return

	}
	_, err := time.Parse("2006-01-02", req.StrDate)
	if err != nil {
		logger.Error("日期格式错误~！")
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
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
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	//计算删除耗时
	t1 := time.Now()
	//脚本删除
	res, err := exec_shell(path)
	if err != nil {
		logger.Errorf("删除文件出错：[%v]", err)
		pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
		return
	}
	durations := time.Since(t1).Seconds()
	_RecordDeleteTotal.WithLabelValues(mp).Inc()
	_RecordDeleteReqDur.WithLabelValues(mp).Observe(durations)
	logger.Infof("Spend time:[%v] 秒, 协程：[%v]", durations, mp)
	logger.Infof("Delete File Success, mountpoint is: [%v], RelativePath is: [%v], loacation is: [%v], RecordID is: [%v], ChannelID is [%v]~~!", req.StrMountPoint, req.StrRelativePath, res, req.StrRecordID, req.StrChannelID)
	pThis.chMQMsg <- cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime}
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
	//chTask <- req
	if ch := pThis.MountPonitTask[req.StrMountPoint]; nil != ch {
		pThis.MountPonitTask[req.StrMountPoint] <- req
		pThis.m_plogger.Infof("分配任务完毕 :[%v]", req.StrMountPoint)
	} else {
		pThis.m_plogger.Infof("分配任务失败:[%v]", req.StrMountPoint)
	}
	return nil
}

func (pThis *ServerStream) goSendMQMsg() {
	logger := LoggerModular.GetLogger()
	for task := range pThis.chMQMsg {
		sendmsg, err := json.Marshal(task)
		if err != nil {
			logger.Errorf("Mashal Failed:[%v]", err)
		}
		err = pThis.pMQConnectPool.SendWithQueue("RecordDelete", false, false, sendmsg, true)
		if err != nil {
			logger.Errorf("Publish Failed: [%v], 协程：[%v]", err, task.StrMountPoint)
		} else {
			logger.Infof("Publish Success~!, 协程：[%v]", task.StrMountPoint)
		}
	}
}

func (pThis *ServerStream) InitServerStream() error {
	pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})

	//获取挂载点
	go pThis.GetMountPoint()

	//链接mq
	pThis.StrMQURL = Config.GetConfig().PublicConfig.AMQPURL
	//pThis.StrMQURL = "amqp://guest:guest@192.168.0.56:30001/"
	//pThis.StrMQURL = "amqp://dengyw:dengyw@49.234.88.77:5672/dengyw"
	size := 1
	pThis.m_plogger.Infof("StrMQURL is: [%v]", pThis.StrMQURL)
	pThis.pMQConnectPool.Init(size, pThis.StrMQURL)
	pThis.chMQMsg = make(chan cli.StreamResData, 102400)

	go pThis.goSendMQMsg()

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

	//开启删除线程
	//pThis.MountPonitTask["/data/yysha002/"] = make(chan *cli.StreamReqData, 50)
	//pThis.MountPonitTask["/data/yysha003/"] = make(chan *cli.StreamReqData, 50)
	for key, chanTask := range pThis.MountPonitTask {
		go pThis.goDeleteFileByMountPoint(key, chanTask)
	}

	//grpc stream服务
	s := grpc.NewServer()
	//注册事件
	cli.RegisterGreeterServer(s, pThis)
	//处理链接
	err = s.Serve(lis)
	if err != nil {
		pThis.m_plogger.Errorf("Process Link Err: [%v]", err)
		return err
	}
	return nil
}
