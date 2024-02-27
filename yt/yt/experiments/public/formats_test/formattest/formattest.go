package formattest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/test/yatest"
	rpcproxypb "go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

const rowsetFile = "rowset_descriptor.pb"

var binaryPath string

func init() {
	var err error
	binaryPath, err = yatest.BinaryPath("yt/yt/experiments/public/formats_test/formats_test")
	if err != nil {
		panic(err)
	}
}

func unpackRefs(blob []byte) ([][]byte, error) {
	b := bytes.NewBuffer(blob)

	var n int32
	if err := binary.Read(b, binary.LittleEndian, &n); err != nil {
		return nil, err
	}

	var attachments [][]byte
	for i := 0; i < int(n); i++ {
		var l int64
		if err := binary.Read(b, binary.LittleEndian, &l); err != nil {
			return nil, err
		}

		blob = make([]byte, int(l))
		attachments = append(attachments, blob)

		if _, err := io.ReadFull(b, blob); err != nil {
			return nil, err
		}
	}

	return attachments, nil
}

func packRefs(attachments [][]byte) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, int32(len(attachments)))

	for _, blob := range attachments {
		_ = binary.Write(&b, binary.LittleEndian, int64(len(blob)))
		_, _ = b.Write(blob)
	}

	return b.Bytes()
}

func EncodeToWire(data []byte, schema *schema.Schema, aggregate, update bool) (
	attachments [][]byte,
	descriptor *rpcproxypb.TRowsetDescriptor,
	err error,
) {
	var schemaStr []byte
	schemaStr, err = yson.MarshalFormat(schema, yson.FormatText)
	if err != nil {
		return
	}

	var out bytes.Buffer

	cmd := exec.Command(
		binaryPath,
		"towire",
		fmt.Sprintf("<aggregate=%%%v;update=%%%v>yson", aggregate, update),
		string(schemaStr),
		"unversioned",
		rowsetFile)
	cmd.Stdin = bytes.NewBuffer(data)
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	if err = cmd.Run(); err != nil {
		return
	}

	attachments, err = unpackRefs(out.Bytes())
	if err != nil {
		err = fmt.Errorf("failed to unpack refs: %w", err)
		return
	}

	var descriptorPb []byte
	descriptorPb, err = os.ReadFile(rowsetFile)
	if err != nil {
		return
	}

	descriptor = &rpcproxypb.TRowsetDescriptor{}
	err = proto.Unmarshal(descriptorPb, descriptor)
	if err != nil {
		err = fmt.Errorf("failed read descriptor: %w", err)
	}
	return
}

func DecodeFromWire(
	attachments [][]byte,
	versioned bool,
	schema *schema.Schema,
	descriptor *rpcproxypb.TRowsetDescriptor,
) ([]byte, error) {
	schemaStr, err := yson.MarshalFormat(schema, yson.FormatText)
	if err != nil {
		return nil, err
	}

	var rowKind string
	if versioned {
		rowKind = "versioned"
	} else {
		rowKind = "unversioned"
	}

	descriptorPb, err := proto.Marshal(descriptor)
	if err != nil {
		return nil, err
	}

	err = os.WriteFile(rowsetFile, descriptorPb, 0666)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer

	cmd := exec.Command(binaryPath, "fromwire", "<format=pretty>yson", string(schemaStr), rowKind, rowsetFile)
	cmd.Stdin = bytes.NewBuffer(packRefs(attachments))
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}
