package server

import (
	cli "StorageMaintainer1/StorageMaintainerGRpc/StorageMaintainerMessage"
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"iPublic/LoggerModular"
	"iPublic/RedisModular"
	"net"
	"os"
	"sync"
)

type Server struct {
	m_mapMountPointLock sync.Mutex
	m_mapMountPoint     map[string]int
	m_strIP             string
	m_nPort             int
	m_RedisCon          *RedisModular.RedisConn
	m_strRedisUrl       string
	m_plogger           *logrus.Entry
	cli.UnimplementedSendServer
}

var server Server

func GetServer() *Server {
	return &server
}

func (pThis *Server) SendMsg(ctx context.Context, req *cli.ReqData) (*cli.ResData, error) {
	if req == nil {
		return nil, errors.New("Empty Msg！~")
	}

	if pThis.m_plogger == nil {
		pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	}
	pThis.m_plogger.Infof("Msg Recived~~~！:[%v]", req)
	return &cli.ResData{NRespond: 1}, nil

	//拼凑需删除的文件夹路径
	path := req.StrMountPoint
	path += req.StrChannelID
	path += "/"
	path += req.StrDate
	pThis.m_plogger.Infof("Path is :[%v]", path)

	//判断路径是否存在
	_, err := os.Stat(path)
	if err != nil {
		pThis.m_plogger.Infof("该设备[%v]文件夹不存在~！", path)
		return nil, err
	}
	return &cli.ResData{StrChannelID: req.StrChannelID, NRespond: 1, StrRecordID: req.StrRecordID, NStartTime: req.NStartTime, StrMountPoint: req.StrMountPoint, StrDate: req.StrDate}, nil
}

func (pThis *Server) InitServer() error {
	pThis.m_plogger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
	//获取挂载点
	//go pThis.GetMountPoint()

	//获取本机IP地址
	ip, err := GetServerStream().GetIPAddres()
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
	err = GetServerStream().WriteToRedis(ip, port)
	if err != nil {
		pThis.m_plogger.Errorf("Write Data to Redis Err: [%v]", err)
		return err
	}
	pThis.m_plogger.Info("Write Data to Redis Success")

	//grpc stream服务
	gRpcServer := grpc.NewServer()
	cli.RegisterSendServer(gRpcServer, &Server{})
	reflection.Register(gRpcServer)
	if err := gRpcServer.Serve(lis); err != nil {
		pThis.m_plogger.Errorf("Failed to Serve Grpc Err: [%v]", err)
		return err
	}

	return nil
}
