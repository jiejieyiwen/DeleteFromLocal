package Config

import (
	"fmt"
	"testing"

	"iPublic/EnvLoad"
)

func TestReadConfig(t *testing.T) {
	c := EnvLoad.GetConf()
	c.MediaConfig = "http://192.168.0.56:8000"
	if err := ReadConfig(); err != nil {
		fmt.Println(err)
	}
	fmt.Println(config)
	//fmt.Println(config.StorageConfig.ConcurrentNumber)
}
