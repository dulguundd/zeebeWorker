package application

import (
	"context"
	"fmt"
	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"
	"github.com/camunda/zeebe/clients/go/v8/pkg/worker"
	"log"
)

func AmsSearchCostumerInformationHandler(client worker.JobClient, job entities.Job) {
	//get Task variable and headers
	jobKey := job.GetKey()
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Logic
	firstname := fmt.Sprint(variables["firstname"])
	lastname := fmt.Sprint(variables["lastname"])
	registerId := fmt.Sprint(variables["registerId"])

	var amsCostumer AmsCostumer

	amsCostumer.costumerId = "0000001"
	amsCostumer.firstname = "dulguun"
	amsCostumer.lastname = "davaadorj"
	amsCostumer.registerId = "99041996"
	var costumerIsSignUp bool
	var costumerRegistrationIsCorrect bool
	if registerId != amsCostumer.registerId {
		costumerIsSignUp = false
	} else {
		costumerIsSignUp = true
	}
	if firstname != amsCostumer.firstname || lastname != amsCostumer.lastname || registerId != amsCostumer.registerId {
		costumerRegistrationIsCorrect = false
	} else {
		costumerRegistrationIsCorrect = true
	}

	//set Task variable
	variables["costumerIsSignUp"] = costumerIsSignUp
	variables["costumerRegistrationIsCorrect"] = costumerRegistrationIsCorrect
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(variables)
	if err != nil {
		// failed to set the updated variables
		failJob(client, job)
		return
	}
	ctx := context.Background()
	_, err = request.Send(ctx)
	if err != nil {
		panic(err)
	}
	log.Println("Successfully completed job")
}

type AmsCostumer struct {
	registerId string
	lastname   string
	firstname  string
	costumerId string
}

func AmsChangeCostumerInformationHandler(client worker.JobClient, job entities.Job) {
	//get Task variable and headers
	jobKey := job.GetKey()
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Logic

	var amsCostumer AmsCostumer

	amsCostumer.costumerId = "0000001"
	amsCostumer.firstname = "dulguun"
	amsCostumer.lastname = "davaadorj"
	amsCostumer.registerId = "99041996"

	//set Task variable
	variables["costumerId"] = amsCostumer.costumerId
	variables["firstname"] = amsCostumer.firstname
	variables["lastname"] = amsCostumer.lastname
	variables["registerId"] = amsCostumer.registerId
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(variables)
	if err != nil {
		// failed to set the updated variables
		failJob(client, job)
		return
	}

	ctx := context.Background()
	_, err = request.Send(ctx)
	if err != nil {
		panic(err)
	}

	log.Println("Successfully completed job")
}

func AmsCreateNewCustomerHandler(client worker.JobClient, job entities.Job) {
	//get Task variable and headers
	jobKey := job.GetKey()
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Logic

	var amsCostumer AmsCostumer

	amsCostumer.costumerId = "0000001"
	amsCostumer.firstname = "dulguun"
	amsCostumer.lastname = "davaadorj"
	amsCostumer.registerId = "99041996"

	//set Task variable
	variables["costumerId"] = amsCostumer.costumerId
	variables["firstname"] = amsCostumer.firstname
	variables["lastname"] = amsCostumer.lastname
	variables["registerId"] = amsCostumer.registerId
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(variables)
	if err != nil {
		// failed to set the updated variables
		failJob(client, job)
		return
	}
	ctx := context.Background()
	_, err = request.Send(ctx)
	if err != nil {
		panic(err)
	}
	log.Println("Successfully completed job")
}

/* Template of Handler
func HANDLER_NAME(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	// GET HEADER
	headers, err := job.GetCustomHeadersAsMap()
	if err != nil {
		// failed to handle job as we require the custom job headers
		failJob(client, job)
		return
	}

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//HERE LOGIC

	// NEW VARIABLES TO TASK
	variables["totalPrice"] = 46.50
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(variables)
	if err != nil {
		// failed to set the updated variables
		failJob(client, job)
		return
	}

	ctx := context.Background()
	_, err = request.Send(ctx)
	if err != nil {
		panic(err)
	}

	//LOG HERE
	log.Println("Successfully completed job")
}
*/
