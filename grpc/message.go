package grpc

import (
	"fmt"
	"math"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

func GetMessageMap(mtd *desc.MethodDescriptor, data *map[string]interface{}) (*dynamic.Message, error) {
	md := mtd.GetInputType()
	if md == nil {
		return nil, fmt.Errorf("No input type of method: %s", mtd.GetName())
	}
	payloadMessage, err := messageFromMap(md, *data)
	if err != nil {
		return nil, err
	}
	return payloadMessage, nil
}

func GetMessageJson(mtd *desc.MethodDescriptor, data string) (*dynamic.Message, error) {
	md := mtd.GetInputType()
	if md == nil {
		return nil, fmt.Errorf("No input type of method: %s", mtd.GetName())
	}
	payloadMessage := dynamic.NewMessage(md)
	if payloadMessage == nil {
		return nil, fmt.Errorf("Couldn't create message from descriptor: %s", md.GetName())
	}
	err := jsonpb.UnmarshalString(data, payloadMessage)
	if err != nil {
		return nil, err
	}
	return payloadMessage, nil
}

func GetMessageBin(mtd *desc.MethodDescriptor, data []byte) (*dynamic.Message, error) {
	md := mtd.GetInputType()
	if md == nil {
		return nil, fmt.Errorf("No input type of method: %s", mtd.GetName())
	}
	payloadMessage := dynamic.NewMessage(md)
	if payloadMessage == nil {
		return nil, fmt.Errorf("Couldn't create message from descriptor: %s", md.GetName())
	}
	buffer := proto.NewBuffer(data)
	err := buffer.DecodeMessage(payloadMessage)
	if err != nil {
		return nil, err
	}
	return payloadMessage, nil
}

func messageFromMap(md *desc.MessageDescriptor, data map[string]interface{}) (*dynamic.Message, error) {
	payloadMessage := dynamic.NewMessage(md)
	if payloadMessage == nil {
		return nil, fmt.Errorf("Couldn't create message from descriptor: %s", md.GetName())
	}
	for k, v := range data {
		fd := md.FindFieldByName(k)
		if fd == nil {
			return nil, fmt.Errorf("Couldn't find field %s in message %s", k, md.GetName())
		}
		if fd.IsMap() {
			keyfd := fd.GetMapKeyType()
			valuefd := fd.GetMapValueType()
			switch v := v.(type) {
			case map[interface{}]interface{}:
				for mk, mv := range v {
					reifiedKey, err := reifyType(keyfd, mk)
					if err != nil {
						return nil, err
					}
					reifiedValue, err := reifyType(valuefd, mv)
					if err != nil {
						return nil, err
					}
					err = payloadMessage.TryPutMapField(fd, reifiedKey, reifiedValue)
					if err != nil {
						return nil, err
					}
				}
			default:
				return nil, fmt.Errorf("Field %s in message is a map, please give it as a map.", k, md.GetName())
			}
		} else if fd.IsRepeated() {
			switch v := v.(type) {
			case []interface{}:
				for _, entry := range v {
					val, err := reifyType(fd, entry)
					if err != nil {
						return nil, err
					}
					err = payloadMessage.TryAddRepeatedField(fd, val)
					if err != nil {
						return nil, err
					}
				}

			default:
				return nil, fmt.Errorf("Field %s in message %s is repeated, make sure it's a list.", k, md.GetName())
			}
		} else {
			reifiedValue, err := reifyType(fd, v)
			if err != nil {
				return nil, err
			}
			err = payloadMessage.TrySetField(fd, reifiedValue)
			if err != nil {
				return nil, err
			}
		}
	}

	return payloadMessage, nil
}

func reifyType(fd *desc.FieldDescriptor, data interface{}) (interface{}, error) {
	md := fd.GetMessageType()
	if md != nil {
		s, ok := data.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("Can't figure out how to parse for field %s", fd.GetFullyQualifiedName())
		}
		res := make(map[string]interface{})
		for k, v := range s {
			res[fmt.Sprintf("%v", k)] = v
		}
		return messageFromMap(md, res)
	}
	int_val, ok := data.(int) //case for enum and int32
	if ok && int_val < math.MaxInt32 && int_val > math.MinInt32 {
		return int32(int_val), nil
	}
	float_val, ok := data.(float64)
	if ok {
		return float32(float_val), nil
	}
	return data, nil
}
