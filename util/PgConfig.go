package util

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

var configFile []byte

type PGConfig struct {
	PG PG `yaml:"pg_config"`
}

type PG struct {
	PgHost     string `yaml:"pg_host"`
	PgPort     string `yaml:"pg_port"`
	PgUser     string `yaml:"pg_user"`
	PgPassword string `yaml:"pg_password"`
	PgDbname   string `yaml:"pg_dbname"`
}

func GetPGConfig() (e *PGConfig, err error) {
	err = yaml.Unmarshal(configFile, &e)
	return e, err
}

func init() {
	var err error
	configFile, err = ioutil.ReadFile("resource/config.yaml")
	if err != nil {
		log.Fatalf("yamlFile.Get err %v ", err)
	}
}
