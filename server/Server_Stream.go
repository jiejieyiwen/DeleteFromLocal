package server

import (
	"Config"
	Mongomon "DeleteFromLocal/Mongo"
	"DeleteFromLocal/MqModular"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net"
	"sync"
	"time"
)

type Record struct {
	IP         string `json:"RecordID" bson:"IP"`
	Path       string `json:"RecordID" bson:"Path"`
	Date       string `json:"RecordID" bson:"Date"`
	MountPoint string `json:"RecordID" bson:"MountPoint"`
	ID         string `json:"RecordID" bson:"ChannelID"`
	LockStatus int    `json:"LockStatus" bson:"LockStatus"`
}

type ServerStream struct {
	m_mapMountPointLock sync.Mutex
	m_mapMountPoint     map[string]int
	m_strIP             string
	m_nPort             int
	m_RedisCon          *RedisModular.RedisConn
	m_strRedisUrl       string
	m_plogger           *logrus.Entry
	UnimplementedGreeterServer

	MountPonitTask     map[string][]*StreamReqData
	MountPonitTaskLock sync.Mutex

	chMQMsg        chan StreamResData
	pMQConnectPool MqModular.ConnPool //MQ连接
	StrMQURL       string             //MQ连接地址

	StorageRedis    *RedisModular.RedisConn
	StorageRedisUrl string

	IP string

	MongoRecord []Record
}

var serverstream ServerStream

func GetServerStream() *ServerStream {
	return &serverstream
}

var ConcurrentNumber int

func (pThis *ServerStream) GettMountPonitTask(mp string) []*StreamReqData {
	pThis.MountPonitTaskLock.Lock()
	defer pThis.MountPonitTaskLock.Unlock()
	a := pThis.MountPonitTask[mp]
	pThis.MountPonitTask[mp] = []*StreamReqData{}
	return a
}

func (pThis *ServerStream) goDeleteFileByMountPoint(mp string) {
	logger := LoggerModular.GetLogger()
	logger.Infof("开启线程：[%v]", mp)
	//chxianliu := make(chan int, Config.GetConfig().StorageConfig.ConcurrentNumber)
	chxianliu := make(chan int, ConcurrentNumber)
	for {
		task := pThis.GettMountPonitTask(mp)
		for _, req := range task {
			chxianliu <- 0
			pThis.goDelete(req.StrMountPoint, req, chxianliu)
		}
		time.Sleep(time.Millisecond * 50)
	}
}

func (pThis *ServerStream) goDelete(mp string, req *StreamReqData, chxianliu chan int) {
	logger := LoggerModular.GetLogger()
	logger.Infof("线程处理任务开始: [%v], [%v]", mp, req)
	if req.StrChannelID == "" || req.StrMountPoint == "" || req.StrDate == "" {
		logger.Errorf("路径错误")
		return
	}
	path := req.StrMountPoint
	path += req.StrChannelID
	path += "/"
	path += req.StrDate
	t1 := time.Now()
	res, err := exec_shell(path)
	if err != nil {
		logger.Errorf("删除文件出错：[%v], Path: [%v], Time: [%v]", err.Error(), path, time.Since(t1).Seconds())
		go Mongomon.GetMongoManager().WriteDeleteFailFileToMongo(pThis.IP, path, req.StrDate, req.StrChannelID, req.StrMountPoint)
		pThis.chMQMsg <- StreamResData{StrChannelID: req.StrChannelID, NRespond: -2, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime, NType: req.NType}
	} else {
		logger.Infof("Spend time:[%v] 秒, 协程：[%v]", time.Since(t1).Seconds(), mp)
		go pThis.goWriteInfoToRedis(time.Since(t1).Seconds(), req.StrMountPoint)
		logger.Infof("Delete File Success, mountpoint is: [%v], Path is: [%v], ChannelID is [%v]~~!", req.StrMountPoint, res, req.StrChannelID)
		pThis.chMQMsg <- StreamResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, StrDate: req.StrDate, StrMountPoint: req.StrMountPoint, NStartTime: req.NStartTime, NType: req.NType}
	}
	<-chxianliu
	logger.Infof("线程处理任务结束：[%v]", mp)
}

func (pThis *ServerStream) goWriteInfoToRedis(durations float64, mp string) {
	t := time.Now().Format("2006-01-02")
	key := "RecordDeleteTotal_"
	key += t
	key += "_"
	key += mp
	if pThis.StorageRedis.Client.Exists(key).Val() == 0 {
		pThis.StorageRedis.Client.Expire(key, time.Hour*72)
	}
	pThis.StorageRedis.Client.Incr(key)

	keys := "RecordDeleteMap_"
	keys += t
	keys += "_"
	keys += mp
	if pThis.StorageRedis.Client.Exists(keys).Val() == 0 {
		pThis.StorageRedis.Client.HSet(keys, "10", 0)
		pThis.StorageRedis.Client.HSet(keys, "20", 0)
		pThis.StorageRedis.Client.HSet(keys, "30", 0)
		pThis.StorageRedis.Client.HSet(keys, "40", 0)
		pThis.StorageRedis.Client.HSet(keys, "50", 0)
		pThis.StorageRedis.Client.HSet(keys, "100", 0)
		pThis.StorageRedis.Client.HSet(keys, "200", 0)
		pThis.StorageRedis.Client.HSet(keys, "INF", 0)
		pThis.StorageRedis.Client.Expire(keys, time.Hour*72)
	}

	if durations <= 10 {
		pThis.StorageRedis.Client.HIncrBy(keys, "10", 1)
	} else if durations > 10 && durations <= 20 {
		pThis.StorageRedis.Client.HIncrBy(keys, "20", 1)
	} else if durations > 20 && durations <= 30 {
		pThis.StorageRedis.Client.HIncrBy(keys, "30", 1)
	} else if durations > 30 && durations <= 40 {
		pThis.StorageRedis.Client.HIncrBy(keys, "40", 1)
	} else if durations > 40 && durations <= 50 {
		pThis.StorageRedis.Client.HIncrBy(keys, "50", 1)
	} else if durations > 50 && durations <= 100 {
		pThis.StorageRedis.Client.HIncrBy(keys, "100", 1)
	} else if durations > 100 && durations <= 200 {
		pThis.StorageRedis.Client.HIncrBy(keys, "200", 1)
	} else {
		pThis.StorageRedis.Client.HIncrBy(keys, "INF", 1)
	}
	pThis.StorageRedis.Client.Expire(key, time.Hour*72)
}

func (pThis *ServerStream) goWriteFileLostInfoToRedis(mp string) {
	t := time.Now().Format("2006-01-02")
	key := "FileLostTotal_"
	key += t
	key += "_"
	key += mp
	if pThis.StorageRedis.Client.Exists(key).Val() == 0 {
		pThis.StorageRedis.Client.Expire(key, time.Hour*72)
	}
	pThis.StorageRedis.Client.Incr(key)
}

func (pThis *ServerStream) GetStream(req *StreamReqData, srv Greeter_GetStreamServer) error {
	if req == nil {
		pThis.m_plogger.Info("Empty Msg！~")
		return errors.New("Empty Msg")
	}
	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	pThis.m_plogger.Infof("Msg Recived~~~！:[%v]", req)
	srv.Send(&StreamResData{NRespond: 1})

	pThis.MountPonitTaskLock.Lock()
	if _, ok := pThis.MountPonitTask[req.StrMountPoint]; ok {
		pThis.MountPonitTask[req.StrMountPoint] = append(pThis.MountPonitTask[req.StrMountPoint], req)
		pThis.m_plogger.Infof("分配任务完毕 :[%v]", req.StrMountPoint)
	} else {
		go pThis.goDeleteFileByMountPoint(req.StrMountPoint)
		pThis.MountPonitTask[req.StrMountPoint] = []*StreamReqData{}
		pThis.MountPonitTask[req.StrMountPoint] = append(pThis.MountPonitTask[req.StrMountPoint], req)
		pThis.m_plogger.Infof("重新分配任务完毕:[%v]", req.StrMountPoint)
	}
	pThis.MountPonitTaskLock.Unlock()
	return nil
}

func (pThis *ServerStream) goSendMQMsg() {
	logger := LoggerModular.GetLogger()
	for task := range pThis.chMQMsg {
		sendmsg, err := json.Marshal(task)
		if err != nil {
			logger.Errorf("Mashal Failed:[%v], 协程：[%v]", err, task.StrMountPoint)
		}
		err = pThis.pMQConnectPool.SendWithQueue("RecordDelete", false, false, sendmsg, true)
		if err != nil {
			logger.Errorf("Publish Failed: [%v], 协程：[%v], Date: [%v]", err, task.StrMountPoint, task.StrDate)
		} else {
			logger.Infof("Publish Success~!, 协程：[%v], Date: [%v]", task.StrMountPoint, task.StrDate)
		}
	}
}

func (pThis *ServerStream) GetFailedFile() {
	logger := LoggerModular.GetLogger()
	err := Mongomon.GetMongoManager().GetDeleteFailFileFromMongo(&pThis.MongoRecord, pThis.IP)
	if err != nil {
		logger.Errorf("GetFailedFile Error: [%v]", err)
		return
	}
	if len(pThis.MongoRecord) == 0 {
		return
	}
	for _, v := range pThis.MongoRecord {
		//计算删除耗时
		t1 := time.Now()
		//脚本删除
		res, err := exec_shell(v.Path)
		if err != nil {
			logger.Errorf("删除文件出错 On Mongo：[%v], Path: [%v]", err.Error(), v.Path)
			time.Sleep(time.Second * 3)
			trtcon := 0
			for {
				_, err1 := exec_shell(v.Path)
				if err1 != nil {
					trtcon++
					if trtcon >= 3 {
						logger.Errorf("无法删除文件夹 On Mongo：[%v], Path: [%v]", err1.Error(), v.Path)
						pThis.chMQMsg <- StreamResData{StrChannelID: v.ID, NRespond: -2, StrRecordID: "", StrDate: v.Date, StrMountPoint: v.MountPoint, NStartTime: 0, NType: int32(v.LockStatus)}
						break
					}
					time.Sleep(time.Second * 3)
					continue
				} else {
					durations := time.Since(t1).Seconds()
					logger.Infof("Spend time:[%v] 秒, 协程：[%v]", durations, v.MountPoint)
					go pThis.goWriteInfoToRedis(durations, v.MountPoint)
					logger.Infof("Delete File Success Again On Mongo, mountpoint is: [%v], Path is: [%v], ChannelID is [%v]~~!", v.MountPoint, v.Path, v.ID)
					pThis.chMQMsg <- StreamResData{StrChannelID: v.ID, NRespond: 1, StrRecordID: "", StrDate: v.Date, StrMountPoint: v.MountPoint, NStartTime: 0, NType: int32(v.LockStatus)}
					data, err1, t := Mongomon.GetMongoManager().DeleteFailFileOnMongo(v.Path)
					if err1 != nil {
						logger.Errorf("Delete DeleteFailFile On Mongo Error: [%v]", err1)
					} else {
						logger.Infof("Delete DeleteFailFile On Mongo Success, Count: [%v], Table: [%v]", data.Removed, t)
					}
					break
				}
			}
		} else {
			durations := time.Since(t1).Seconds()
			logger.Infof("Spend time:[%v] 秒, 协程：[%v]", durations, v.MountPoint)
			go pThis.goWriteInfoToRedis(durations, v.MountPoint)
			logger.Infof("Delete File Success On Mongo, mountpoint is: [%v], Path is: [%v], ChannelID is [%v]~~!", v.MountPoint, res, v.ID)
			pThis.chMQMsg <- StreamResData{StrChannelID: v.ID, NRespond: 1, StrRecordID: "", StrDate: v.Date, StrMountPoint: v.MountPoint, NStartTime: 0, NType: int32(v.LockStatus)}
			data, err1, t := Mongomon.GetMongoManager().DeleteFailFileOnMongo(v.Path)
			if err1 != nil {
				logger.Errorf("Delete DeleteFailFile On Mongo Error: [%v]", err1)
				continue
			} else {
				logger.Infof("Delete DeleteFailFile On Mongo Success, Count: [%v], Table: [%v]", data.Removed, t)
			}
		}
	}
}

func (pThis *ServerStream) InitServerStream() error {
	pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})

	//链接mq
	pThis.StrMQURL = Config.GetConfig().PublicConfig.AMQPURL
	//pThis.StrMQURL = "amqp://guest:guest@192.168.0.56:30001/"
	//pThis.StrMQURL = "amqp://dengyw:dengyw@49.234.88.77:5672/dengyw"
	size := 1
	pThis.m_plogger.Infof("StrMQURL is: [%v]", pThis.StrMQURL)
	pThis.pMQConnectPool.Init(size, pThis.StrMQURL)
	pThis.chMQMsg = make(chan StreamResData, 1024000)

	go pThis.goSendMQMsg()

	//获取本机IP地址
	var err error
	pThis.IP, err = pThis.GetIPAddres()
	if err != nil {
		pThis.m_plogger.Errorf("Get IP Address Err: [%v]", err)
		return err
	}

	//建立grpc监听
	lis, err := net.Listen("tcp", ":41010")
	if err != nil {
		pThis.m_plogger.Errorf("Start GRPC Listen Err: [%v]", err)
		return err
	}
	pThis.m_plogger.Info("Start GRPC Listen Success")

	//获取端口
	//port := lis.Addr().(*net.TCPAddr).Port
	port := 41010

	//写入redis
	err = pThis.Initedis()
	if err != nil {
		return err
	}
	go pThis.goUpdateMountPoint(pThis.IP, port)

	//grpc stream服务
	s := grpc.NewServer()
	//注册事件
	RegisterGreeterServer(s, pThis)
	//处理链接
	err = s.Serve(lis)
	if err != nil {
		pThis.m_plogger.Errorf("Process Link Err: [%v]", err)
		return err
	}
	return nil
}
