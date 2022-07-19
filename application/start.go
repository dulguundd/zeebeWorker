package application

import (
	"context"
	"fmt"
	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"
	"github.com/camunda/zeebe/clients/go/v8/pkg/worker"
	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"
	"log"
)

var readyClose = make(chan struct{})

var JobTypeString = [...]string{
	"ams-search-costumer-information-service",
	"ams-change-costumer-information-service",
	"ams-create-new-custumer-service",
	"check-customer",
	"get-customer-orders",
}

func Start() {
	environment := getEnvironment()
	zbClient, err := zbc.NewClient(&zbc.ClientConfig{
		GatewayAddress:         environment.zeebeConfig.zeebeAddress,
		UsePlaintextConnection: true,
	})
	if err != nil {
		panic(err)
	}

	jobWorkerType := zbClient.NewJobWorker().JobType(environment.serviceConfig.workerType)

	switch environment.serviceConfig.workerType {
	case JobTypeString[0]: //if workerType == "payment-service"
		jobWorker := jobWorkerType.Handler(AmsSearchCostumerInformationHandler).Open()
		closeJobWorker(jobWorker, environment.serviceConfig.workerType)
	case JobTypeString[1]: //if workerType == "inventory-service"
		jobWorker := jobWorkerType.Handler(AmsChangeCostumerInformationHandler).Open()
		closeJobWorker(jobWorker, environment.serviceConfig.workerType)
	case JobTypeString[2]:
		jobWorker := jobWorkerType.Handler(AmsCreateNewCustomerHandler).Open()
		closeJobWorker(jobWorker, environment.serviceConfig.workerType)
	case JobTypeString[3]:
		jobWorker := jobWorkerType.Handler(CheckCustomer).Open()
		closeJobWorker(jobWorker, environment.serviceConfig.workerType)
	case JobTypeString[4]:
		jobWorker := jobWorkerType.Handler(GetCustomerOrders).Open()
		closeJobWorker(jobWorker, environment.serviceConfig.workerType)
	default:
		fmt.Printf("No matched worker type! \n")
		return
	}
}

func closeJobWorker(jobWorker worker.JobWorker, workerType string) {
	fmt.Printf("Starting worker type: " + workerType + "\n")

	<-readyClose
	jobWorker.Close()
	jobWorker.AwaitClose()

}

func failJob(client worker.JobClient, job entities.Job) {
	log.Println("Failed to complete job", job.GetKey())

	ctx := context.Background()
	_, err := client.NewFailJobCommand().JobKey(job.GetKey()).Retries(job.Retries - 1).Send(ctx)
	if err != nil {
		panic(err)
	}
}
