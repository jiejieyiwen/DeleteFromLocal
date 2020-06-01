package server

import (
	"bytes"
	"os"
	"os/exec"
)

func exec_shell(path string) (string, error) {
	command := "./delete.sh " + path
	cmd := exec.Command("/bin/bash", "-c", command)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		//logger.Error(err.Error())
		return "", err
	}
	return string(out.Bytes()), nil
}

func DeleteByShell(path string) (string, error) {
	command := "./delete.sh " + path
	cmd := exec.Command("/bin/bash", "-c", command)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		//logger.Error(err.Error())
		return "", err
	}
	return string(out.Bytes()), nil
}

func removeObject(fpath string) error {
	return os.RemoveAll(fpath)
}

func getMountPoint() (string, error) {
	command := "./mountinfo.sh "
	cmd := exec.Command("/bin/bash", "-c", command)

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		//logger.Error(err.Error())
		return "", err
	}
	return string(out.Bytes()), nil
}
