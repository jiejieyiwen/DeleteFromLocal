package server

import (
	cli "StorageMaintainer/StorageMaintainerGRpc/StorageMaintainerMessage"
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
	//cli.UnimplementedDelete_NotificationServer
	cli.UnimplementedGreeterServer
}

var chtask chan *cli.StreamReqData

var result []*cli.StreamResData
var resultLock sync.Mutex

func SetResult(data *cli.StreamResData) {
	resultLock.Lock()
	defer resultLock.Unlock()
	result = append(result, data)
}

func GetResult() []*cli.StreamResData {
	resultLock.Lock()
	defer resultLock.Unlock()
	a := result
	result = []*cli.StreamResData{}
	return a
}

var serverstream ServerStream

func GetServerStream() *ServerStream {
	return &serverstream
}

func godeleteTask(req *cli.StreamReqData) {
	logger := LoggerModular.GetLogger()
	if len(req.StrMountPoint) == 0 {
		logger.Error("无挂载点~！")
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		SetResult(data)
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		//srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return
	}
	if len(req.StrChannelID) == 0 {
		logger.Error("无设备ID~！")
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		SetResult(data)
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		//srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return
	}
	if len(req.StrChannelID) < 19 {
		logger.Error("设备ID长度错误~！")
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		SetResult(data)
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		//srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return
	}
	if len(req.StrDate) == 0 {
		logger.Error("无日期~！")
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		SetResult(data)
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		//srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
		return
	}
	_, err := time.Parse("2006-01-02", req.StrDate)
	if err != nil {
		logger.Error("日期格式错误~！")
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		SetResult(data)
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID}
		//srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -1, StrRecordID: req.StrRecordID})
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
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID}
		SetResult(data)
		//err1 := srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate})
		//if err1 != nil {
		//	logger.Errorf("回复文件夹不存在消息失败:[%v]", err1)
		//	return
		//} else {
		//	logger.Infof("回复文件夹不存在消息成功~！")
		//	return
		//}
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate}
		return
	}

	//计算删除耗时
	t1 := time.Now()
	//脚本删除
	res, err := exec_shell(path)
	if err != nil {
		logger.Errorf("删除文件出错：[%v]", err)
		data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID}
		SetResult(data)
		//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID, StrDate: req.StrDate}
		//err1 := srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID, StrDate: req.StrDate})
		//if err1 != nil {
		//	logger.Errorf("回复删除文件出错消息失败:[%v]", err1)
		//	return
		//} else {
		//	logger.Infof("回复删除文件出错消息成功~！")
		//	return
		//}
		return
	}
	t2 := time.Now()
	logger.Infof("Spend time:[%v] 毫秒", t2.Sub(t1).Milliseconds())
	logger.Infof("Delete File Success, mountpoint is: [%v], RelativePath is: [%v], loacation is: [%v], RecordID is: [%v], ChannelID is [%v]~~!",
		req.StrMountPoint, req.StrRelativePath, res, req.StrRecordID, req.StrChannelID)

	data := &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID}
	SetResult(data)

	//回复消息
	//chtask1 <- &cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, NStartTime: req.NStartTime, StrMountPoint: req.StrMountPoint}
	//err = srv.Send(&cli.StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, NStartTime: req.NStartTime, StrMountPoint: req.StrMountPoint})
	//if err != nil {
	//	logger.Errorf("回复删除成功消息失败:[%v]", err)
	//	return
	//} else {
	//	logger.Infof("回复删除成功消息成功~！")
	//}
}

func goGetTask() {
	for task := range chtask {
		go godeleteTask(task)
	}
}

func (pThis *ServerStream) GetStream(req *cli.StreamReqData, srv cli.Greeter_GetStreamServer) error {
	if req == nil {
		return errors.New("Empty Msg！~")
	}

	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	pThis.m_plogger.Infof("Msg Recived~~~！:[%v]", req)

	chtask <- req

	res := GetResult()
	for _, v := range res {
		err := srv.Send(&cli.StreamResData{StrChannelID: v.StrChannelID, NRespond: v.NRespond, StrRecordID: v.StrRecordID, NStartTime: v.NStartTime, StrMountPoint: v.StrMountPoint})
		if err != nil {
			pThis.m_plogger.Errorf("回复删除成功消息失败:[%v]", err)
			return errors.New("回复删除成功消息失败~!")
		} else {
			pThis.m_plogger.Infof("回复删除成功消息成功~！")
		}
	}
	return nil
}

func (pThis *ServerStream) InitServerStream() error {
	pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	//获取挂载点
	go pThis.GetMountPoint()

	chtask = make(chan *cli.StreamReqData, 10)

	go goGetTask()

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
