package Config

import (
	"fmt"
	"testing"

	"iPublic/EnvLoad"
)

func TestReadConfig(t *testing.T) {
	c := EnvLoad.GetConf()
	c.MediaConfig = "http://config.mj.cn/imccp-mediacore"
	if err := ReadConfig(); err != nil {
		fmt.Println(err)
	}
	fmt.Println(config)
	//fmt.Println(config.StorageConfig.ConcurrentNumber)
}
