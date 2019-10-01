package orderbooks


import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"code.cryptowat.ch/cw-sdk-go/common"

	"fmt"
	"os"
)

type Delta struct {
	Year   int
	Title  string
	Plot   string
	Rating float64
}

type OrderBookWriter struct {
	Session *session.Session
	Client *dynamodb.DynamoDB
}

func NewOrderBookWriter() *OrderBookWriter {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create DynamoDB client
	cli := dynamodb.New(sess)
	return  &OrderBookWriter{
		Session: sess,
		Client: cli,
	}
}

func (obw *OrderBookWriter) writeDelta(obd common.OrderBookDelta) {

	fmt.Println("writing delta")

	return
	item := Delta{
		Year:   2015,
		Title:  "The Big New Movie",
		Plot:   "Nothing happens at all.",
		Rating: 0.0,
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("Got error marshalling new movie item:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("tableName"),
	}

	_, err = obw.Client.PutItem(input)
	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return
}

