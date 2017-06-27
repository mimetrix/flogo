package kafka

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TIBCOSoftware/flogo-lib/core/action"
	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const producerAddr = "localhost:9092"
const topic = "testing-flogo-trigger"

func getJsonMetadata() string {
	jsonMetadataBytes, err := ioutil.ReadFile("trigger.json")
	if err != nil {
		panic("No Json Metadata found for trigger.json path")
	}
	return string(jsonMetadataBytes)
}

var didRun = false

type TestRunner struct {
}

// Run implements action.Runner.Run
func (tr *TestRunner) Run(context context.Context, action action.Action, uri string, options interface{}) (code int, data interface{}, err error) {
	didRun = true
	return 0, nil, nil
}

func getRandConf() (conf, value string) {
	value = strconv.Itoa(int(rand.Uint32()))
	conf = `{
	  "id": "kafka-json",
	  "settings": {
	    "broker": "` + producerAddr + `"
	  },
	  "handlers": [
	    {
	      "actionId": "test_action",
	      "settings": {
	        "topic": "` + topic + `",
	        "jsonPath": "some.test",
	        "jsonValue": "` + value + `"
	      }
	    }
	  ]
	}`

	return
}

func TestInit(t *testing.T) {

	// New factory
	md := trigger.NewMetadata(getJsonMetadata())
	f := NewFactory(md)

	// New Trigger
	config := trigger.Config{}
	conf, _ := getRandConf()
	json.Unmarshal([]byte(conf), &config)
	tgr := f.New(&config)

	runner := &TestRunner{}

	tgr.Init(runner)
}

func TestMessageOk(t *testing.T) {

	// New factory
	md := trigger.NewMetadata(getJsonMetadata())
	f := NewFactory(md)

	// New Trigger
	config := trigger.Config{}
	conf, value := getRandConf()
	log.Println("inserting value:", value)
	json.Unmarshal([]byte(conf), &config)
	tgr := f.New(&config)

	runner := &TestRunner{}

	tgr.Init(runner)

	tgr.Start()
	defer tgr.Stop()

	producer, err := sarama.NewSyncProducer([]string{producerAddr}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if er := producer.Close(); er != nil {
			log.Fatalln(er)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(`{ "some": { "test": "` + value + `" } }`)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}

	time.Sleep(5)

	if !didRun {
		t.Fatal("Did not catch message")
	}
}
