package application

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"
	"github.com/camunda/zeebe/clients/go/v8/pkg/worker"
	"github.com/dulguundd/logError-lib/logger"
	"github.com/jackc/pgx/v4"
	"github.com/streadway/amqp"
	"log"
	"reflect"
	"strconv"
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

func ResponseWriter(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	customerId := variables["customerId"]
	response := variables["respone"]
	replyTo := variables["replyTo"]
	correlationId := variables["correlationId"]

	responseJson, err := json.Marshal(response)
	if err != nil {
		logger.Error("Convert error")
	}
	replyToString := fmt.Sprintf("%v", replyTo)
	correlationIdString := fmt.Sprintf("%v", correlationId)

	err = rabbitmqResponse(correlationIdString, replyToString, responseJson)
	if err != nil {
		failJob(client, job)
	}
	// NEW VARIABLES TO TASK
	ctx := context.Background()
	_, err = client.NewCompleteJobCommand().JobKey(jobKey).Send(ctx)
	if err != nil {
		// failed to set the updated variables
		failJob(client, job)
		return
	}
	//LOG HERE
	log.Printf("Successfully completed job of id: %d\n", int(customerId.(float64)))
}
func rabbitmqResponse(correlationId string, replyTo string, response []byte) error {
	conn, err := amqp.Dial("amqp://guest:guest@172.30.52.239:5672/")
	if err != nil {
		logger.Error("Failed to connect to RabbitMQ")
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logger.Error("Failed to open a channel")
		return err
	}
	defer ch.Close()
	err = ch.Publish(
		"",      // exchange
		replyTo, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: correlationId,
			Body:          response,
		})
	if err != nil {
		logger.Error("Failed to connect to RabbitMQ")
		return err
	} else {
		logger.Info("Instance Response send, correlationId: " + correlationId)
		return nil
	}
}

func GetColorId(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	colorName := variables["colorName"]
	var colorId int

	//DB connection

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@172.22.2.215:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	err = conn.QueryRow(context.Background(), "select id from webshop.colors where name = $1",
		colorName).Scan(&colorId)

	if err != nil {
		logger.Error("Error while querying data table " + err.Error())
		failJob(client, job)
		return
	}

	// NEW VARIABLES TO TASK
	variables["colorId"] = colorId

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
	log.Printf("Successfully completed job of color id: %d\n", colorId)
}

func GetSizeId(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	gender := variables["gender"]
	size := variables["size"]
	var sizeId int

	//DB connection

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@172.22.2.215:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	err = conn.QueryRow(context.Background(), "select id from webshop.sizes where size = $1 AND gender = $2",
		size, gender).Scan(&sizeId)

	if err != nil {
		logger.Error("Error while querying data table " + err.Error())
		failJob(client, job)
		return
	}

	// NEW VARIABLES TO TASK
	variables["sizeId"] = sizeId

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
	log.Printf("Successfully completed job of size id: %d\n", sizeId)
}

func GetProductListByColorAndSize(client worker.JobClient, job entities.Job) {
	jobKey := job.GetKey()

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	colorId := variables["colorId"]
	sizeId := variables["sizeId"]
	var productId int
	var productIds = []int{}

	//DB connection

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@172.22.2.215:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(),
		"select id from webshop.articles where colorid = $1 and size = $2", colorId, sizeId)

	if err != nil {
		logger.Error("Error while querying data table " + err.Error())
		failJob(client, job)
		return
	}

	for rows.Next() {
		rows.Scan(&productId)
		productIds = append(productIds, productId)
	}
	if err != nil {
		logger.Error("Error while querying data table " + err.Error())
		failJob(client, job)
		return
	}

	// NEW VARIABLES TO TASK
	variables["productIds"] = productIds

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

func GetProductsStockCount(client worker.JobClient, job entities.Job) {
	start := time.Now()
	jobKey := job.GetKey()

	// GET VARIABLES OF TASK
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		// failed to handle job as we require the variables
		failJob(client, job)
		return
	}

	//Get variables form Zeebe
	productIdsMap := variables["productIds"]
	productIds := zeebeVariableToArrayInt(productIdsMap)
	var stockCounts []int
	var stockCount int

	//DB connection

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@172.22.2.215:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	for _, productId := range productIds {
		err = conn.QueryRow(context.Background(), "select count from webshop.stock where articleid = $1",
			productId).Scan(&stockCount)

		if err != nil {
			logger.Error("Error while querying data table " + err.Error())
			failJob(client, job)
			return
		}
		stockCounts = append(stockCounts, stockCount)
	}

	// NEW VARIABLES TO TASK
	variables["stockCounts"] = stockCounts

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
	elapsed := time.Since(start)
	log.Printf("response latencie %s", elapsed)
	log.Printf("Successfully completed job")
}

func zeebeVariableToArrayInt(t interface{}) []int {
	var response []int
	var strVar string
	switch reflect.TypeOf(t).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(t)

		for i := 0; i < s.Len(); i++ {
			strVar = fmt.Sprint(s.Index(i))
			intVar, _ := strconv.Atoi(strVar)
			response = append(response, intVar)
		}
	}
	return response
}
