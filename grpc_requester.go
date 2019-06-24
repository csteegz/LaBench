package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"labench/bench"
	"strings"

	"github.com/bojand/ghz/protodesc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GRPCRequesterFactory struct {
	Host         string                 `yaml:"Host"`
	Call         string                 `yaml:"Call"`
	ShareChannel bool                   `yaml:"ShareChannel"`
	Data         map[string]interface{} `yaml:"Data"`
	Header       map[string]string      `yaml:"Header"`
	Proto        string                 `yaml:"Proto"`
	Protoset     string                 `yaml:"Protoset"`
	ImportPaths  []string               `yaml:"ImportPaths"`

	channel *grpc.ClientConn
}

func (g *GRPCRequesterFactory) GetChannel(reuse bool) (*grpc.ClientConn, error) {
	var err error
	if !reuse {
		return grpc.Dial(g.Host, grpc.WithInsecure())
	}
	if g.channel == nil {
		g.channel, err = grpc.Dial(g.Host, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
	}
	return g.channel, nil
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (g *GRPCRequesterFactory) GetRequester(uint64) bench.Requester {
	var mtd *desc.MethodDescriptor
	var connection *grpc.ClientConn
	var err error
	if g.Proto != "" {
		mtd, err = protodesc.GetMethodDescFromProto(g.Call, g.Proto, g.ImportPaths)
	} else if g.Protoset != "" {
		mtd, err = protodesc.GetMethodDescFromProtoSet(g.Call, g.Protoset)
	} else {
		err = errors.New("Couldn't parse proto type.")
	}
	if err != nil {
		panic(err)
	}
	connection, err = g.GetChannel(g.ShareChannel)
	if err != nil {
		panic(err)
	}

	md := mtd.GetInputType()
	payloadMessage := dynamic.NewMessage(md)
	if payloadMessage == nil {
		panic(fmt.Errorf("No input type of method: %s", mtd.GetName()))
	}
	err = messageFromMap(payloadMessage, &g.Data)
	if err != nil {
		panic(err)
	}
	return &GRPCRequester{stub: grpcdynamic.NewStub(connection), mtd: mtd, message: payloadMessage, headers: metadata.New(g.Header)}
}

// GRPC Requestor is limited to unary-unary. Streams have harder synchronization requirements.
type GRPCRequester struct {
	stub    grpcdynamic.Stub
	mtd     *desc.MethodDescriptor
	message *dynamic.Message

	headers metadata.MD
	ctx     context.Context
}

func (w *GRPCRequester) Setup() error {
	w.ctx = metadata.NewOutgoingContext(context.Background(), w.headers)
	return nil
}

func (w *GRPCRequester) Request() error {
	_, err := w.stub.InvokeRpc(w.ctx, w.mtd, w.message, grpc.Header(&w.headers))
	if err != nil {
		return err
	}
	return nil
}

func (w *GRPCRequester) Teardown() error { return nil }

// from bojand/ghz/runner/data.go
func messageFromMap(input *dynamic.Message, data *map[string]interface{}) error {
	strData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = jsonpb.UnmarshalString(string(strData), input)
	if err != nil {
		return err
	}

	return nil
}

func createPayloadsFromJSON(data string, mtd *desc.MethodDescriptor) ([]*dynamic.Message, error) {
	md := mtd.GetInputType()
	var inputs []*dynamic.Message

	if len(data) > 0 {
		if strings.IndexRune(data, '[') == 0 {
			dataArray := make([]map[string]interface{}, 5)
			err := json.Unmarshal([]byte(data), &dataArray)
			if err != nil {
				return nil, fmt.Errorf("Error unmarshalling payload. Data: '%v' Error: %v", data, err.Error())
			}

			elems := len(dataArray)
			if elems > 0 {
				inputs = make([]*dynamic.Message, elems)
			}

			for i, elem := range dataArray {
				elemMsg := dynamic.NewMessage(md)
				err := messageFromMap(elemMsg, &elem)
				if err != nil {
					return nil, fmt.Errorf("Error creating message: %v", err.Error())
				}

				inputs[i] = elemMsg
			}
		} else {
			inputs = make([]*dynamic.Message, 1)
			inputs[0] = dynamic.NewMessage(md)
			err := jsonpb.UnmarshalString(data, inputs[0])
			if err != nil {
				return nil, fmt.Errorf("Error creating message from data. Data: '%v' Error: %v", data, err.Error())
			}
		}
	}

	return inputs, nil
}
