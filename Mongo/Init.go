package Mongomon

import (
	"Config"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"iPublic/LoggerModular"
	"iPublic/MongoModular"
	"strconv"
	"time"
)

type MongoManager struct {
	Con      MongoModular.MongoDBServ
	Table    string
	Logger   *logrus.Entry
	MongoURL string
}

var mongoConManager MongoManager

func GetMongoManager() *MongoManager {
	return &mongoConManager
}

func init() {
	mongoConManager.Table = "NoExistFile"
	mongoConManager.Logger = LoggerModular.GetLogger().WithFields(logrus.Fields{})
}

func (pThis *MongoManager) Init() error {
	pThis.MongoURL = Config.GetConfig().MongoDBConfig.MongoDBURLMongo

	pThis.MongoURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@192.168.2.64:27017/mj_log?authSource=admin&maxPoolSize=100"
	//pThis.MongoURL = "mongodb://mj_ya_admin:EkJcQeOP$bGh8IYC@127.0.0.1:15677/mj_log?authSource=admin&maxPoolSize=100"

	pThis.Logger.Infof("Mongo Url Is: [%v]", pThis.MongoURL)
	if err := MongoModular.GetMongoDBHandlerWithURL(pThis.MongoURL, &pThis.Con); err != nil {
		pThis.Logger.Errorf("Init Mongo Connect Error: [%v]", err)
		return err
	} else {
		pThis.Logger.Infof("Init Mongo Connect Success, Url: [%v]", pThis.MongoURL)
	}
	return nil
}

func (pThis *MongoManager) WriteNoExistFileToMongo(ip, path, date string) {
	baseFilter := []interface{}{bson.M{"IP": ip, "Path": path, "Date": date, "CreateTime": strconv.Itoa(int(time.Now().Unix()))}}
	err := pThis.Con.Insert("NoExistFile", baseFilter)
	if err != nil {
		pThis.Logger.Error(err)
		return
	}
	pThis.Logger.Info("Write NoExistFile Success")
}

func (pThis *MongoManager) WriteDeleteFailFileToMongo(ip, path, date, id, mp string) {
	baseFilter := []interface{}{bson.M{"IP": ip, "Path": path, "Date": date, "ChannelID": id, "MountPoint": mp, "CreateTime": time.Now().Format("2006-01-02")}}
	err := pThis.Con.Insert("DeleteFailFile", baseFilter)
	if err != nil {
		pThis.Logger.Error(err)
		return
	}
	pThis.Logger.Info("Write DeleteFailFile Success")
}

func (pThis *MongoManager) GetDeleteFailFileFromMongo(tpl interface{}, ip string) error {
	baseFilter := []interface{}{bson.M{"IP": ip}}
	filter := bson.M{"$and": baseFilter}
	return pThis.Con.FindAll("DeleteFailFile", filter, "+CreateTime", 0, 0, tpl)
}

func (pThis *MongoManager) DeleteFailFileOnMongo(path string) (info *mgo.ChangeInfo, err error, table string) {
	baseFilter := []interface{}{bson.M{"Path": path}}
	filter := bson.M{"$and": baseFilter}
	t := "DeleteFailFile"
	info, err = pThis.Con.DeleteAll(t, filter)
	return info, err, t
}
