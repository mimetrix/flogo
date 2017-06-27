package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/TIBCOSoftware/flogo-lib/core/action"
	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/qiniu/log"
	"github.com/tidwall/gjson"
)

// KafkaTriggerFactory My Trigger factory
type KafkaTriggerFactory struct {
	metadata *trigger.Metadata
}

//NewFactory create a new Trigger factory
func NewFactory(md *trigger.Metadata) trigger.Factory {
	return &KafkaTriggerFactory{metadata: md}
}

//New Creates a new trigger instance for a given id
func (t *KafkaTriggerFactory) New(config *trigger.Config) trigger.Trigger {
	return &KafkaTrigger{metadata: t.metadata, config: config}
}

// KafkaTrigger is a stub for your Trigger implementation
type KafkaTrigger struct {
	metadata           *trigger.Metadata
	runner             action.Runner
	config             *trigger.Config
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	partitionClosers   []chan bool
}

// Init implements trigger.Trigger.Init
func (t *KafkaTrigger) Init(runner action.Runner) {
	if t.config.Settings == nil {
		panic(fmt.Sprintf("No Settings found for trigger '%s'", t.config.Id))
	}

	if _, ok := t.config.Settings["broker"]; !ok {
		panic(fmt.Sprintf("No Broker found for trigger '%s' in settings", t.config.Id))
	}

	// Init handlers
	for _, handler := range t.config.Handlers {

		if handler.Settings == nil || handler.Settings["topic"] == "" || handler.Settings["jsonPath"] == "" || handler.Settings["jsonValue"] == "" {
			panic(fmt.Sprintf("Invalid handler: %v", handler))
		}
	}

	t.runner = runner
}

// Metadata implements trigger.Trigger.Metadata
func (t *KafkaTrigger) Metadata() *trigger.Metadata {
	return t.metadata
}

// Start implements trigger.Trigger.Start
func (t *KafkaTrigger) Start() error {
	broker := t.config.GetSetting("broker")

	var err error
	t.consumer, err = sarama.NewConsumer([]string{broker}, nil)
	if err != nil {
		panic(err)
	}

	// Init handlers
	for _, handler := range t.config.Handlers {

		topic := handler.GetSetting("topic")
		jsonPath := handler.GetSetting("jsonPath")
		jsonValue := handler.GetSetting("jsonValue")

		log.Debugf("Kafka Trigger: Registering handler [%s: %s == %s] for Action Id: [%s]", topic, jsonPath, jsonValue, handler.ActionId)

		partitionConsumer, er := t.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if er != nil {
			panic(er)
		}
		t.partitionConsumers = append(t.partitionConsumers, partitionConsumer)

		close := make(chan bool)
		t.partitionClosers = append(t.partitionClosers, close)

		go func(partitionConsumer sarama.PartitionConsumer, jsonPath, jsonValue, actionId string) {
			consumed := 0
		ConsumerLoop:
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					log.Debugf("Consumed message offset %d\n", msg.Offset)
					consumed++
					json := string(msg.Value)
					value := gjson.Get(json, jsonPath)
					if value.Exists() && value.String() == jsonValue {
						data := map[string]interface{}{
							"message": json,
						}
						startAttrs, _ := t.metadata.OutputsToAttrs(data, false)

						action := action.Get(actionId)
						log.Debugf("Found action' %+x'", action)

						context := trigger.NewContext(context.Background(), startAttrs)
						_, _, err := t.runner.Run(context, action, actionId, nil)

						if err != nil {
							log.Debugf("Kafka trigger Action Error: %s", err.Error())
							return
						}
					}
					println()
				case <-close:
					break ConsumerLoop
				}
			}

			log.Printf("Consumed: %d\n", consumed)
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
			close <- true
		}(partitionConsumer, jsonPath, jsonValue, handler.ActionId)

	}

	// start the trigger
	return nil
}

// Stop implements trigger.Trigger.Start
func (t *KafkaTrigger) Stop() error {
	// Signal to all partitions to close
	for _, closer := range t.partitionClosers {
		closer <- true
	}
	// Wait for each partition to close
	for _, closer := range t.partitionClosers {
		<-closer
	}

	// Close parent consummer
	if err := t.consumer.Close(); err != nil {
		log.Fatalln(err)
	}
	return nil
}
