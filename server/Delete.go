package server

import (
	"bytes"
	"iPublic/LoggerModular"
	"os/exec"
)

func exec_shell(path string) (string, error) {
	logger := LoggerModular.GetLogger()
	command := "./delete.sh " + path
	cmd := exec.Command("/bin/bash", "-c", command)

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		logger.Error(err)
		return "", err
	}
	return string(out.Bytes()), nil
}
