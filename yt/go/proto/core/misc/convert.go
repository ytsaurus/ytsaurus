package misc

import (
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func NewErrorFromProto(proto *TError) error {
	if proto == nil {
		return nil
	}

	var err yterrors.Error
	if proto.Code != nil {
		err.Code = yterrors.ErrorCode(*proto.Code)
	}
	err.Message = proto.GetMessage()

	if proto.Attributes != nil {
		err.Attributes = map[string]interface{}{}

		for _, protoAttr := range proto.Attributes.Attributes {
			if protoAttr.Key == nil || protoAttr.Value == nil {
				continue
			}

			var attr interface{}
			if yson.Unmarshal(protoAttr.Value, &attr) != nil {
				err.Attributes[*protoAttr.Key] = yson.RawValue(protoAttr.Value)
			} else {
				err.Attributes[*protoAttr.Key] = attr
			}
		}
	}

	for _, inner := range proto.InnerErrors {
		err.InnerErrors = append(err.InnerErrors, NewErrorFromProto(inner).(*yterrors.Error))
	}

	if err.Code != 0 {
		return &err
	} else {
		return nil
	}
}

func NewGUIDFromProto(proto *TGuid) guid.GUID {
	return guid.FromHalves(proto.GetFirst(), proto.GetSecond())
}

func NewProtoFromGUID(g guid.GUID) *TGuid {
	a, b := g.Halves()
	return &TGuid{
		First:  &a,
		Second: &b,
	}
}
