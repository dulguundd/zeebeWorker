package application

import (
	"context"
	"fmt"
	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"
	"github.com/camunda/zeebe/clients/go/v8/pkg/worker"
	"github.com/dulguundd/logError-lib/logger"
	"github.com/jackc/pgx/v4"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type CheckCustomerDBResponse struct {
	id     int    `db:"id"`
	gender string `db:"gender"`
	email  string `db:"email"`
}

type OrdersDBResponse struct {
	id                int
	customer          int
	ordertimestamp    time.Time
	shippingaddressid int
	total             string
	shippingcost      string
	created           time.Time
	updated           time.Time
}

func CheckCustomer(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	var customerInfo CheckCustomerDBResponse

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	firstname := variables["firstname"]
	lastname := variables["lastname"]

	//DB connection

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@172.22.2.215:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	err = conn.QueryRow(context.Background(), "select id, gender, email from webshop.customer where firstname = $1 AND lastname = $2",
		firstname, lastname).Scan(&customerInfo.id, &customerInfo.gender, &customerInfo.email)

	if err != nil {
		logger.Error("Error while querying data table " + err.Error())
		failJob(client, job)
		return
	}

	// NEW VARIABLES TO TASK
	variables["customerId"] = customerInfo.id
	variables["gender"] = customerInfo.gender
	variables["email"] = customerInfo.email

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
	log.Printf("Successfully completed job of id: %d\n", customerInfo.id)
}

func GetCustomerOrders(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	var orderList OrdersDBResponse

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	customerId := variables["customerId"]
	orderCount := 0
	orderIds := []int{}
	//DB connection

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@172.22.2.215:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(),
		"select id from webshop.order where customer = $1", customerId)

	for rows.Next() {
		rows.Scan(&orderList.id)
		orderIds = append(orderIds, orderList.id)
		orderCount++
	}
	fmt.Println(orderIds)
	fmt.Println(orderCount)

	if err != nil {
		logger.Error("Error while querying data table " + err.Error())
		failJob(client, job)
		return
	}

	// NEW VARIABLES TO TASK
	variables["orderIds"] = orderIds
	variables["orderCount"] = orderCount
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
	log.Printf("Successfully completed job of id: %d\n", int(customerId.(float64)))
}
