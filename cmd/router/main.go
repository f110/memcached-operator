package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/f110/memcached-operator/logger"
	"github.com/f110/memcached-operator/router"
	"github.com/go-yaml/yaml"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func Router(args []string) error {
	l, err := zap.NewProduction()
	if err != nil {
		return err
	}
	logger.Log = l.Sugar()

	confFile := "/etc/router/router.yaml"
	fs := flag.NewFlagSet("router", flag.ContinueOnError)
	fs.StringVar(&confFile, "c", confFile, "conf file path")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	conf, err := loadConfig(confFile)
	if err != nil {
		return errors.WithStack(err)
	}

	r := router.NewRouter(":11211", conf.ToRouter())
	return r.ListenAndServe()
}

func loadConfig(path string) (*router.Config, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf router.Config
	if err := yaml.Unmarshal(b, &conf); err != nil {
		return nil, err
	}

	return &conf, nil
}

func main() {
	if err := Router(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%+v", err)
		if logger.Log != nil {
			logger.Log.Errorf("%+v", err)
		}
		os.Exit(1)
	}
}
