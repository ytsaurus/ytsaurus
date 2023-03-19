package rpcclient

import (
	"reflect"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/wire"
)

const CurrentWireFormatVersion = 1

func encodeToWire(items []interface{}) ([][]byte, *rpc_proxy.TRowsetDescriptor, error) {
	nameTable, rows, err := wire.Encode(items)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to encode keys: %w", err)
	}

	t, err := convertNameTable(nameTable)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to convert name table: %w", err)
	}

	kind := rpc_proxy.ERowsetKind_RK_UNVERSIONED
	descriptor := &rpc_proxy.TRowsetDescriptor{
		WireFormatVersion: ptr.Int32(CurrentWireFormatVersion),
		RowsetKind:        &kind,
		RowsetFormat:      nil,
		NameTableEntries:  t,
		Schema:            nil,
	}

	data, err := wire.MarshalRowset(rows)
	if err != nil {
		return nil, nil, err
	}

	attachments := [][]byte{data}

	return attachments, descriptor, nil
}

func encodePivotKeys(keys interface{}) ([][]byte, error) {
	if keys == nil {
		return nil, nil
	}

	vv := reflect.ValueOf(keys)
	if vv.Kind() != reflect.Slice {
		return nil, xerrors.Errorf("unsupported keys type: %v", vv.Kind())
	}

	items := make([]interface{}, 0, vv.Len())
	for i := 0; i < vv.Len(); i++ {
		items = append(items, vv.Index(i).Interface())
	}

	rows, err := wire.EncodePivotKeys(items)
	if err != nil {
		return nil, err
	}

	data, err := wire.MarshalRowset(rows)
	if err != nil {
		return nil, err
	}

	attachments := [][]byte{data}

	return attachments, err
}

func convertNameTable(t wire.NameTable) ([]*rpc_proxy.TRowsetDescriptor_TNameTableEntry, error) {
	nameTable := make([]*rpc_proxy.TRowsetDescriptor_TNameTableEntry, 0, len(t))
	for _, k := range t {
		entry := &rpc_proxy.TRowsetDescriptor_TNameTableEntry{
			Name: ptr.String(k.Name),
		}

		nameTable = append(nameTable, entry)
	}
	return nameTable, nil
}

func decodeFromWire(attachments [][]byte) ([]wire.Row, error) {
	var ret []wire.Row

	rows, err := wire.UnmarshalRowset(mergeAttachments(attachments))
	if err != nil {
		return nil, xerrors.Errorf("unable to deserialize attachments: %w", err)
	}
	ret = append(ret, rows...)

	return ret, nil
}

func mergeAttachments(attachments [][]byte) []byte {
	var merged []byte
	for _, a := range attachments {
		merged = append(merged, a...)
	}
	return merged
}

func makeNameTable(d *rpc_proxy.TRowsetDescriptor) (wire.NameTable, error) {
	table := make(wire.NameTable, 0, len(d.NameTableEntries))

	for _, e := range d.NameTableEntries {
		if e.Name == nil {
			return nil, xerrors.Errorf("unexpected name table entry with nil name: %+v", e)
		}

		if e.Type == nil {
			return nil, xerrors.Errorf("unexpected name table entry with nil name: %+v", e)
		}

		entry := wire.NameTableEntry{
			Name: *e.Name,
		}

		table = append(table, entry)
	}

	return table, nil
}
