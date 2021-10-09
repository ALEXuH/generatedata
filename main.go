package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/juju/ratelimit"
	"github.com/spf13/viper"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LineSplit   = ":"
	ColumnIndex = 0
	TypeIndex   = 1
	ValueIndex  = 2
	STAND       = "2006-01-02 15-04-05" // yyyy-mm-dd HH:mm:ss
	TimeFormat  = "2006-01-02 15:04:05" // yyyy-mm-dd HH:mm:ss
	TimeHistory = "20060102150405"      // yyyy-mm-dd HH:mm:ss
)

var logger = log.Default()
var container = make(map[string][]interface{}, 1000)
var fields = make(map[string]int, 0)
var data = make(chan string, 200000)
var concurrentTime = make([]string, 0)
var randInt = make([][]string, 0)
var randFloat = make([][]string, 0)
var randString = make([][]string, 0)
var faker = gofakeit.NewCrypto()
var WG sync.WaitGroup
var config *Config
var kafkaCount int32 = 0

type Config struct {
	FieldConfigName string
	DataFormat      string
	OutType         string
	OutputFileName  string
	History         int
	Total           int
	Cpu             int
	InputThreads    int
	OutputThreads   int
	Broker          string
	Topic           string
	Speed           int
}

type SyncWriter struct {
	L      sync.Mutex
	Writer *bufio.Writer
}

func (w *SyncWriter) writer(value string) {
	w.L.Lock()
	defer w.L.Unlock()
	if _, err := w.Writer.WriteString(value); err != nil {
		logger.Println(" write err ", err, err)
	}
	if _, err := w.Writer.WriteString("\n"); err != nil {
		logger.Println(" write err ", err, err)
	}
}

func init() {
	// 读取config配置文件
	viper.SetConfigName("config")                 // name of config file (without extension)
	viper.SetConfigType("ini")                    // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(filepath.Dir(os.Args[0])) // optionally look for config in the working directory
	//viper.AddConfigPath("C:\\Users\\think\\go\\src\\producer\\") // optionally look for config in the working directory
	logger.Println(" current run path:", filepath.Dir(os.Args[0]))
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	config = &Config{
		FieldConfigName: filepath.Join(filepath.Dir(os.Args[0]), viper.GetString("input.field_config_file")),
		//FieldConfigName: "C:\\Users\\think\\go\\src\\producer\\fields.ini",
		DataFormat:     strings.ToLower(viper.GetString("format.format")),
		OutType:        strings.ToLower(viper.GetString("output.outType")),
		OutputFileName: viper.GetString("output.output_file"),
		History:        viper.GetInt("output.history"),
		Total:          viper.GetInt("input.total"),
		Speed:          viper.GetInt("input.speed"),
		Cpu:            viper.GetInt("system.cpu"),
		InputThreads:   viper.GetInt("system.input_threads"),
		OutputThreads:  viper.GetInt("system.output_threads"),
		Broker:         viper.GetString("kafka.brokers"),
		Topic:          viper.GetString("kafka.topic"),
	}

	//logger.Println(config)
	// 初始化数据池
	fieldInit(config.FieldConfigName)
}

func fieldInit(filename string) {
	logger.Println("init value beginning")
	// 读取配置文件
	inputFile, err := os.Open(filename)
	if err != nil {
		logger.Panicln("read config file error : %s", err.Error())
		return
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			logger.Println(" input file close err ", err)
		}
	}()
	bufferIo := bufio.NewReader(inputFile)
	// 解析配置文件初始化数据
	for {
		v, err := bufferIo.ReadString('\n')
		values := strings.Split(v, LineSplit)
		column := values[ColumnIndex]
		logger.Println("column:", column)
		value := strings.TrimSpace(values[ValueIndex])
		cloumnType := strings.ToLower(strings.TrimSpace(values[TypeIndex]))

		if cloumnType == "struct" {
			slice := convert2Slice(str2Slice(values[2]))
			container[column] = slice
			fields[column] = len(slice)
		} else if cloumnType == "int" {
			randValue := make([]string, 3)
			randValue[0] = strings.TrimSpace(column)
			randValue[1] = strings.TrimSpace(strings.Split(value, ",")[0])
			randValue[2] = strings.TrimSpace(strings.Split(value, ",")[1])
			randInt = append(randInt, randValue)
		} else if cloumnType == "float" {
			randValue := make([]string, 3)
			randValue[0] = strings.TrimSpace(column)
			randValue[1] = strings.TrimSpace(strings.Split(value, ",")[0])
			randValue[2] = strings.TrimSpace(strings.Split(value, ",")[1])
			randFloat = append(randFloat, randValue)
		} else if cloumnType == "string" {
			randValue := make([]string, 2)
			randValue[0] = strings.TrimSpace(column)
			randValue[1] = strings.TrimSpace(value)
			randString = append(randString, randValue)
		} else if cloumnType == "datetime" {
			if value == "now()" {
				concurrentTime = append(concurrentTime, column)
			} else {
				d := strings.Split(value, ",")
				startTime, err := time.Parse(STAND, strings.TrimSpace(d[0]))
				endTime, err1 := time.Parse(STAND, strings.TrimSpace(d[1]))
				if err != nil || err1 != nil {
					logger.Println("error time column ", column)
				} else {
					length, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
					if err != nil {
						length = 100
						//logger.Println(" WARNING can not parse int value",err)
					}
					valueChannel := make([]interface{}, length)
					for i := 0; i < int(length); i++ {
						valueChannel[i] = faker.DateRange(startTime, endTime).Format(TimeFormat)
					}
					container[column] = valueChannel
					fields[column] = int(length)
					//fmt.Println(fields, container)
				}

			}
		} else {
			length, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
			if err != nil {
				length = 100
				logger.Println(" WARNING can not parse int value")
			}
			valueChannel := make([]interface{}, length)
			switch cloumnType {
			case "username":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Username()
				}
			case "email":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Email()
				}
			case "ip4":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.IPv4Address()
				}
			case "ip6":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.IPv6Address()
				}
			case "url":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.URL()
				}
			case "phone":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Phone()
				}
			case "uuid":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.UUID()
				}
			case "domain":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.DomainName()
				}
			case "macaddress":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.MacAddress()
				}
			case "address":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Address()
				}
			case "country":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Country()
				}
			case "city":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.City()
				}
			case "street":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Street()
				}
			case "game":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Gamertag()
				}
			case "fruit":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Fruit()
				}
			case "httpcode":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.HTTPStatusCode()
				}
			case "httpmethod":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.HTTPMethod()
				}
			case "useragent":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.UserAgent()
				}
			case "company":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.Company()
				}
			case "joblevel":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.JobLevel()
				}
			case "jobtitle":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.JobTitle()
				}
			case "jobdescriptor":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.JobDescriptor()
				}
			case "appname":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.AppName()
				}
			case "appversion":
				for i := 0; i < int(length); i++ {
					valueChannel[i] = faker.AppVersion()
				}
			default:
				logger.Println("error type ", cloumnType)
			}
			container[column] = valueChannel
			fields[column] = int(length)
		}

		if err == io.EOF && strings.ContainsAny(v, ":") {
			logger.Println("READ OVER")
			break
		}
		logger.Println("init value over start faker data")
	}
	logger.Println("init value over start faker data")
}

func main() {
	runtime.GOMAXPROCS(config.Cpu)
	logger.Println("total cpu num", runtime.NumCPU(), "user cpu nums ", config.Cpu)
	limit := ratelimit.NewBucketWithRate(float64(config.Speed), int64(config.Speed))
	for i := 0; i <= config.InputThreads; i++ {
		go produceData(limit)
	}

	// 存储数据池下次使用
	go generateFieldConfig()

	if config.Total > 0 && config.OutType == "file" {
		logger.Println("start file out")
		writer, err := os.OpenFile(config.OutputFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			logger.Println("file write fail. . .")
		}
		defer func() {
			if err := writer.Close(); err != nil {
				logger.Println("close err ", err)
			}
		}()

		syncWriter := &SyncWriter{sync.Mutex{}, bufio.NewWriter(writer)}
		WG.Add(1)
		go consumer2File(syncWriter, config.Total)
		WG.Wait()
		logger.Println("file out over")
	} else if config.OutType == "kafka" {
		WG.Add(1)
		logger.Println(" start kafka out type streaming ... ")
		KafkaConfig := sarama.NewConfig()
		KafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
		KafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
		KafkaConfig.Producer.Return.Successes = true
		KafkaConfig.Producer.Retry.Max = 3
		KafkaConfig.Version = sarama.V0_10_0_0
		KafkaConfig.ClientID = "FakerData"

		client, err := sarama.NewSyncProducer(strings.Split(config.Broker, ","), KafkaConfig)
		if err != nil {
			logger.Fatal(" Producer err ", err.Error())
			return
		}
		logger.Println("start ...")
		for i := 0; i <= config.OutputThreads; i++ {
			WG.Add(1)
			//logger.Println("aaa")
			go consumer2Kafka(client, config.Topic)
		}
		WG.Add(1)
		go func() {
			tricker := time.NewTicker(1 * time.Second)
			s := 1
			defer WG.Done()
			for {
				select {
				case <-tricker.C:
					logger.Println("total count:", kafkaCount, " speed:", kafkaCount/int32(s), "time:", s, "s")
					s++
				}
			}
		}()
		WG.Wait()
	} else {
		logger.Panicln("can not support out type")
	}
}

func produceData(limit *ratelimit.Bucket) {
	for {
		value := make(map[string]interface{})
		for k, v := range fields {
			value[k] = container[k][rand.Intn(v)]
		}
		for _, v := range concurrentTime {
			value[v] = time.Now().Format(TimeFormat)
		}
		for _, v := range randInt {
			v1 := -99999
			v2 := 99999
			if x, err := strconv.ParseInt(v[1], 10, 64); err != nil {
				logger.Println("int range parse error ", x)
			} else {
				v1 = int(x)
			}
			if x1, err := strconv.ParseInt(v[2], 10, 64); err != nil {
				logger.Println("int range parse error ", v2)
			} else {
				v2 = int(x1)
			}
			value[v[0]] = faker.Number(v1, v2)
		}

		for _, v := range randFloat {
			v1 := -99999.99
			v2 := 99999.99
			if x1, err := strconv.ParseFloat(v[1], 64); err != nil {
				logger.Println("flaot range parse error ", x1)
			} else {
				v1 = x1
			}
			if x2, err := strconv.ParseFloat(v[2], 64); err != nil {
				logger.Println("int range parse error ", x2)
			} else {
				v2 = x2
			}
			value[v[0]] = faker.Float64Range(v1, v2)
		}
		for _, v := range randString {
			v1 := 10
			if x1, err := strconv.ParseFloat(v[1], 64); err != nil {
				logger.Println("int range parse error ", v1)
			} else {
				v1 = int(x1)
			}
			value[v[0]] = faker.DigitN(uint(v1))
		}

		jsonString, _ := json.Marshal(value)
		//logger.Println(string(jsonString))
		limit.Wait(1)
		data <- string(jsonString)
	}
}

func consumer2File(syncWriter *SyncWriter, total int) {
	count := 0
	defer WG.Done()
	for {
		select {
		case d := <-data:
			syncWriter.writer(d)
			count++
			if count >= total {
				if err := syncWriter.Writer.Flush(); err != nil {
					logger.Println(" flush err ", err)
				}
				return
			}
		}
	}
}

func consumer2Kafka(client sarama.SyncProducer, topic string) {
	defer WG.Done()
	defer func() {
		if err := client.Close(); err != nil {
			logger.Println(" close err ", err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case d := <-data:
			_, _, err := client.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(d)})
			if err != nil {
				logger.Println(err)
			}
			atomic.AddInt32(&kafkaCount, 1)
		case <-signals:
			if err := client.Close(); err != nil {
				logger.Println(" cloent close err ", err)
			}
		}
	}
}

func generateFieldConfig() {
	logger.Println("start generate field")
	fileName := time.Now().Format(TimeHistory) + "Fields.ini"
	writer, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	file := bufio.NewWriter(writer)
	if err != nil {
		logger.Println("file write fail. . .")
	}
	for k, v := range container {
		columnValues := make([]string, len(v))
		for ix, columnValue := range v {
			switch columnValue.(type) {
			case string:
				columnValues[ix] = columnValue.(string)
			default:
				logger.Println("ignore field:", k)
			}
		}
		//fmt.Println(strings.Join(columnValues, ","))
		if _, err := file.WriteString(k + ":struct:" + strings.Join(columnValues, ",")); err != nil {
			logger.Println("write err ", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			logger.Println(" write err ", err)
		}
	}
	for _, curret := range concurrentTime {
		if _, err := file.WriteString(curret + ":dateTime:now()"); err != nil {
			logger.Println(" write error ", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			logger.Println(" write err ", err)
		}
	}
	for _, curret := range randInt {
		fmt.Sprintf("%s:%s:%s", curret[0], curret[1], curret[2])
		if _, err := file.WriteString(fmt.Sprintf("%s:int:%s,%s", curret[0], curret[1], curret[2])); err != nil {
			logger.Println(" write error ", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			logger.Println(" write err ", err)
		}
	}
	for _, curret := range randFloat {
		if _, err := file.WriteString(fmt.Sprintf("%s:float:%s,%s", curret[0], curret[1], curret[2])); err != nil {
			logger.Println(" write error ", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			logger.Println(" write err ", err)
		}
	}
	for _, curret := range randString {
		if _, err := file.WriteString(fmt.Sprintf("%s:string:%s", curret[0], curret[1])); err != nil {
			logger.Println(" write error ", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			logger.Println(" write err ", err)
		}
	}
	if err := file.Flush(); err != nil {
		logger.Println(" flush err ", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			logger.Println("close err", err)
		}
	}()
	logger.Println("generate field over")
}

func str2Slice(a string) []interface{} {
	slice := strings.Split(strings.TrimSpace(a), ",")
	value := make([]interface{}, len(slice))
	for ix, v := range slice {
		value[ix] = v
	}
	return value
}

// 判断是否为切片
func isSlince(arg interface{}) (reflect.Value, bool) {
	val := reflect.ValueOf(arg)
	ok := false
	if val.Kind() == reflect.Slice {
		ok = true
	}
	return val, ok
}

// 转化为[]interface{}（装载任意元素的切片与[]int这种不兼容） 切片
func convert2Slice(arg interface{}) []interface{} {
	val, ok := isSlince(arg)
	if !ok {
		return nil
	}
	length := val.Len()
	out := make([]interface{}, length)
	for i := 0; i < length; i++ {
		out[i] = val.Index(i).Interface()
	}
	return out
}
