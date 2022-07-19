package application

import (
	"os"
)

type Environment struct {
	serviceConfig ServiceConfig
	zeebeConfig   ZeebeConfig
}

type ServiceConfig struct {
	workerType string
}

type ZeebeConfig struct {
	zeebeAddress string
}

func getEnvironment() *Environment {
	var environment Environment
	environment = Environment{
		zeebeConfig: struct{ zeebeAddress string }{zeebeAddress: "10.12.7.29:26500"},
	}

	if os.Getenv("WORKER_TYPE") != "" {
		environment.serviceConfig.workerType = os.Getenv("WORKER_TYPE")
	}

	if os.Getenv("ZEEBE_ADDRESS") != "" {
		environment.zeebeConfig.zeebeAddress = os.Getenv("ZEEBE_ADDRESS")
	}

	return &environment
}
