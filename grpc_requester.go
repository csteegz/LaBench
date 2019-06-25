package main

import (
	"context"
	"errors"
	"labench/bench"
	prsgrpc "labench/grpc"
	"sync"

	"github.com/bojand/ghz/protodesc"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GRPCRequesterFactory struct {
	Host         string                  `yaml:"Host"`
	Call         string                  `yaml:"Call"`
	ShareChannel bool                    `yaml:"ShareChannel"`
	Data         *map[string]interface{} `yaml:"Data"`
	Header       map[string]string       `yaml:"Header"`
	Proto        string                  `yaml:"Proto"`
	Protoset     string                  `yaml:"Protoset"`
	ImportPaths  []string                `yaml:"ImportPaths"`
	DataJSON     string                  `yaml:"DataJSON"`
	DataBin      []byte                  `yaml:"DataBin"`

	mux     sync.Mutex
	channel *grpc.ClientConn
	md      *desc.MethodDescriptor
	message *dynamic.Message
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

func (g *GRPCRequesterFactory) GetMethodDesc() (*desc.MethodDescriptor, error) {
	var mtd *desc.MethodDescriptor
	var err error

	if g.md != nil {
		return g.md, nil
	}
	g.mux.Lock()
	defer g.mux.Unlock()
	if g.Proto != "" && g.Protoset == "" {
		mtd, err = protodesc.GetMethodDescFromProto(g.Call, g.Proto, g.ImportPaths)
	} else if g.Protoset != "" && g.Proto == "" {
		mtd, err = protodesc.GetMethodDescFromProtoSet(g.Call, g.Protoset)
	} else {
		err = errors.New("Couldn't parse proto type. Must have exactly one of Proto and Protoset set.")
	}
	g.md = mtd
	return mtd, err
}

func (g *GRPCRequesterFactory) GetRequestProto(mtd *desc.MethodDescriptor) (*dynamic.Message, error) {

	var payloadMessage *dynamic.Message
	var err error
	if g.message != nil {
		return g.message, nil
	}
	g.mux.Lock()
	defer g.mux.Unlock()
	if g.Data != nil {
		payloadMessage, err = prsgrpc.GetMessageMap(mtd, g.Data)
	} else if g.DataJSON != "" {
		payloadMessage, err = prsgrpc.GetMessageJson(mtd, g.DataJSON)
	} else if len(g.DataBin) != 0 {
		payloadMessage, err = prsgrpc.GetMessageBin(mtd, g.DataBin)
	} else {
		err = errors.New("Couldn't get body data. Must set one of Data, DataJSON or DataBin")
	}
	g.message = payloadMessage
	return payloadMessage, err
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (g *GRPCRequesterFactory) GetRequester(uint64) bench.Requester {

	var connection *grpc.ClientConn
	var payloadMessage *dynamic.Message
	var err error
	mtd, err := g.GetMethodDesc()
	if err != nil {
		panic(err)
	}
	connection, err = g.GetChannel(g.ShareChannel)
	if err != nil {
		panic(err)
	}
	payloadMessage, err = g.GetRequestProto(mtd)
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
