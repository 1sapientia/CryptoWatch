package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/common"
	"code.cryptowat.ch/cw-sdk-go/proto/kafka"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"log"
	"time"
)

type Item interface {
	ConvertForKafka(topic string, exchange string, pair string) (*sarama.ProducerMessage)
}

type Trade struct {
	Timestamp float64 //market id
	Price     float64 //timestamp (ms)
	Amount    float64 //amount (positive bid, negative ask)
}

//GenerateProducerMessage generates Kafka message to send via producer
func (ord Trade) ConvertForKafka(topic string, exchange string, pair string) *sarama.ProducerMessage {

	timestampProto, _ := ptypes.TimestampProto(time.Unix(0, int64(ord.Timestamp)))
	messageKey := &kafka.MessageKey{
		Exchange: common.FixExchangeName(exchange),
		Pair:     common.FixPair(pair),
	}

	messageValue := &kafka.MessageValue{
		Price:     float32(ord.Price),
		Amount:    float32(ord.Amount),
		Timestamp: timestampProto,
	}

	key, err1 := proto.Marshal(messageKey)
	value, err2 := proto.Marshal(messageValue)
	if err1 != nil || err2 != nil {
		log.Println("Proto marshaling failed", err1, err2)
	}

	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}

type Delta struct {
	Timestamp float64 //market id
	Price     float64 //timestamp (ms)
	Amount    float64 //amount (positive bid, negative ask)
}

//GenerateProducerMessage generates Kafka message to send via producer
func (del Delta) ConvertForKafka(topic string, exchange string, pair string) *sarama.ProducerMessage {

	timestampProto, _ := ptypes.TimestampProto(time.Unix(0, int64(del.Timestamp)))
	messageKey := &kafka.MessageKey{
		Exchange: common.FixExchangeName(exchange),
		Pair:     common.FixPair(pair),
	}

	messageValue := &kafka.MessageValue{
		Price:     float32(del.Price),
		Amount:    float32(del.Amount),
		Timestamp: timestampProto,
	}

	key, err1 := proto.Marshal(messageKey)
	value, err2 := proto.Marshal(messageValue)
	if err1 != nil || err2 != nil {
		log.Println("Proto marshaling failed", err1, err2)
	}

	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}
