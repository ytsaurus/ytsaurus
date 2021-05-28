package rpcclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yt"
)

func TestConvertObjectType(t *testing.T) {
	for _, tc := range []struct {
		typ      yt.NodeType
		expected ObjectType
		err      bool
	}{
		{typ: yt.NodeMap, expected: ObjectTypeMapNode},
		{typ: yt.NodeLink, expected: ObjectTypeLink},
		{typ: yt.NodeFile, expected: ObjectTypeFile},
		{typ: yt.NodeTable, expected: ObjectTypeTable},
		{typ: yt.NodeString, expected: ObjectTypeStringNode},
		{typ: yt.NodeBoolean, expected: ObjectTypeBooleanNode},
		{typ: yt.NodeDocument, expected: ObjectTypeDocument},
		{typ: yt.NodeTableReplica, expected: ObjectTypeTableReplica},
		{typ: yt.NodeReplicatedTable, expected: ObjectTypeReplicatedTable},
		{typ: yt.NodeUser, expected: ObjectTypeUser},
		{typ: yt.NodeGroup, expected: ObjectTypeGroup},
		{typ: yt.NodeAccount, expected: ObjectTypeAccount},
		{typ: yt.NodeMedium, expected: ObjectTypeMedium},
		{typ: yt.NodeTabletCellBundle, expected: ObjectTypeTabletCellBundle},
		{typ: yt.NodeSys, expected: ObjectTypeSysNode},
		{typ: yt.NodePortalEntrance, expected: ObjectTypePortalEntrance},
		{typ: yt.NodePortalExit, expected: ObjectTypePortalExit},
		{typ: yt.NodeSchedulerPool, expected: ObjectTypeSchedulerPool},
		{typ: yt.NodeSchedulerPoolTree, expected: ObjectTypeSchedulerPoolTree},
		{typ: yt.NodeType("invalid"), err: true},
	} {
		t.Run(string(tc.typ), func(t *testing.T) {
			objType, err := convertObjectType(tc.typ)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, objType)
			}
		})
	}
}
