module labench

require (
	github.com/bojand/ghz v0.37.0
	github.com/jhump/protoreflect v1.1.0
	golang.org/x/net v0.0.0-20190522155817-f3200d17e092
	google.golang.org/grpc v1.17.0
	gopkg.in/yaml.v2 v2.2.2
	labench/bench v0.0.0
)

replace labench/bench => ./bench
