package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"time"

	"labench/bench"

	yaml "gopkg.in/yaml.v2"
)

type benchParams struct {
	RequestRatePerSec uint64        `yaml:"RequestRatePerSec"`
	Clients           uint64        `yaml:"Clients"`
	Duration          time.Duration `yaml:"Duration"`
	BaseLatency       time.Duration `yaml:"BaseLatency"`
	RequestTimeout    time.Duration `yaml:"RequestTimeout"`
	ReuseConnections  bool          `yaml:"ReuseConnections"`
	DontLinger        bool          `yaml:"DontLinger"`
	OutputJSON        bool          `yaml:"OutputJSON"`
	TightTicker       bool          `yaml:"TightTicker"`
}

type config struct {
	Params      benchParams           `yaml:",inline"`
	Protocol    string                `yaml:"Protocol"`
	Request     *WebRequesterFactory  `yaml:"Request"`
	GRPCRequest *GRPCRequesterFactory `yaml:"GRPCRequest"`
}

func maybePanic(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func assert(cond bool, err string) {
	if !cond {
		log.Panic(errors.New(err))
	}
}

func prepareRequestorFactory(conf *config) bench.RequesterFactory {

	if conf.Params.RequestTimeout == 0 {
		conf.Params.RequestTimeout = 10 * time.Second
	}

	if conf.Params.Clients == 0 {
		clients := conf.Params.RequestRatePerSec * uint64(math.Ceil(conf.Params.RequestTimeout.Seconds()))
		clients += clients / 5 // add 20%
		conf.Params.Clients = clients
		fmt.Println("Clients:", clients)
	}

	if conf.Protocol == "" {
		conf.Protocol = "HTTP/1.1"
	}

	fmt.Println("Protocol:", conf.Protocol)

	switch conf.Protocol {
	case "GRPC":
		assert(conf.GRPCRequest != nil, "Tried to use GRPC but didn't provide the request infromation.")
		return conf.GRPCRequest
	case "HTTP/2":
		initHTTP2Client(conf.Params.RequestTimeout, conf.Params.DontLinger)
	case "HTTP/1.1":
		initHTTPClient(conf.Params.ReuseConnections, conf.Params.RequestTimeout, conf.Params.DontLinger)
	default:
		log.Panic("Invalid protocol - must be GRPC, HTTP/2 or HTTP/1.1")
	}
	assert(conf.Request != nil, "Didn't provide any request information.")
	return conf.Request
}

func main() {
	configFile := "labench.yaml"
	if len(os.Args) > 1 {
		assert(len(os.Args) == 2, fmt.Sprintf("Usage: %s [config.yaml]\n\tThe default config file name is: %s", os.Args[0], configFile))
		configFile = os.Args[1]
	}

	configBytes, err := ioutil.ReadFile(configFile)
	maybePanic(err)

	var conf config
	err = yaml.Unmarshal(configBytes, &conf)
	maybePanic(err)

	// fmt.Printf("%+v\n", conf)
	fmt.Println("timeStart =", time.Now().UTC().Add(-5*time.Second).Truncate(time.Second))
	factory := prepareRequestorFactory(&conf)
	benchmark := bench.NewBenchmark(factory, conf.Params.RequestRatePerSec, conf.Params.Clients, conf.Params.Duration, conf.Params.BaseLatency)
	summary, err := benchmark.Run(conf.Params.OutputJSON, conf.Params.TightTicker)
	maybePanic(err)

	fmt.Println("timeEnd   =", time.Now().UTC().Add(5*time.Second).Round(time.Second))

	fmt.Println(summary)

	err = os.MkdirAll("out", os.ModeDir|os.ModePerm)
	maybePanic(err)

	err = summary.GenerateLatencyDistribution(bench.Logarithmic, path.Join("out", "res.hgrm"))
	maybePanic(err)
}
