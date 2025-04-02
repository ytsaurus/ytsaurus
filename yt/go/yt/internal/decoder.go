package internal

type resultDecoder func(res *CallResult) error
type valueResultDecoder[T any] func(value T) resultDecoder
type anyValueResultDecoder = valueResultDecoder[any]

func newSingleValueResultDecoder(key string) anyValueResultDecoder {
	return func(value any) resultDecoder {
		return func(res *CallResult) error {
			return res.decodeSingle(key, value)
		}
	}
}

func newValueResultDecoder() anyValueResultDecoder {
	return func(value any) resultDecoder {
		return func(res *CallResult) error {
			return res.decode(value)
		}
	}
}

func newJSONValueResultDecoder() anyValueResultDecoder {
	return func(value any) resultDecoder {
		return func(res *CallResult) error {
			return res.decodeJSON(value)
		}
	}
}

func newRawValueResultDecoder() valueResultDecoder[*[]byte] {
	return func(value *[]byte) resultDecoder {
		return func(res *CallResult) error {
			*value = res.YSONValue
			return nil
		}
	}
}

var (
	noopResultDecoder         resultDecoder         = func(res *CallResult) error { return nil }
	CreateNodeResultDecoder   anyValueResultDecoder = newSingleValueResultDecoder("node_id")
	CreateObjectResultDecoder anyValueResultDecoder = newSingleValueResultDecoder("object_id")
	NodeExistsResultDecoder   anyValueResultDecoder = newSingleValueResultDecoder("value")
	GetNodeResultDecoder      anyValueResultDecoder = newSingleValueResultDecoder("value")
	ListNodeResultDecoder     anyValueResultDecoder = newSingleValueResultDecoder("value")
	CopyMoveNodeResultDecoder anyValueResultDecoder = newSingleValueResultDecoder("node_id")
	LinkNodeResultDecoder     anyValueResultDecoder = newSingleValueResultDecoder("node_id")
	LockNodeResultDecoder     anyValueResultDecoder = newValueResultDecoder()
	// The whoami method returns JSON value, not YSON, so we need to specify the decoding.
	WhoAmIResultDecoder                     anyValueResultDecoder       = newJSONValueResultDecoder()
	IssueTokenResultDecoder                 anyValueResultDecoder       = newValueResultDecoder()
	ListUserTokensResultDecoder             anyValueResultDecoder       = newValueResultDecoder()
	BuildMasterSnapshotsResultDecoder       anyValueResultDecoder       = newValueResultDecoder()
	BuildSnapshotResultDecoder              anyValueResultDecoder       = newValueResultDecoder()
	AddMaintenanceResultDecoder             anyValueResultDecoder       = newValueResultDecoder()
	RemoveMaintenanceResultDecoder          anyValueResultDecoder       = newValueResultDecoder()
	CheckPermissionResultDecoder            anyValueResultDecoder       = newValueResultDecoder()
	StartTxResultDecoder                    anyValueResultDecoder       = newSingleValueResultDecoder("transaction_id")
	StartTabletTxResultDecoder              anyValueResultDecoder       = newSingleValueResultDecoder("transaction_id")
	StartOperationResultDecoder             anyValueResultDecoder       = newSingleValueResultDecoder("operation_id")
	GetOperationResultDecoder               anyValueResultDecoder       = newValueResultDecoder()
	GetOperationByAliasResultDecoder        anyValueResultDecoder       = newValueResultDecoder()
	ListOperationsResultDecoder             anyValueResultDecoder       = newValueResultDecoder()
	ListJobsResultDecoder                   anyValueResultDecoder       = newValueResultDecoder()
	GetJobStderrResultDecoder               valueResultDecoder[*[]byte] = newRawValueResultDecoder()
	PutFileToCacheResultDecoder             anyValueResultDecoder       = newValueResultDecoder()
	GetFileFromCacheResultDecoder           anyValueResultDecoder       = newValueResultDecoder()
	PushQueueProducerResultDecoder          anyValueResultDecoder       = newValueResultDecoder()
	PushQueueProducerBatchResultDecoder     anyValueResultDecoder       = newValueResultDecoder()
	CreateQueueProducerSessionResultDecoder anyValueResultDecoder       = newValueResultDecoder()
	DisableChunkLocationsResultDecoder      anyValueResultDecoder       = newValueResultDecoder()
	DestroyChunkLocationsResultDecoder      anyValueResultDecoder       = newValueResultDecoder()
	ResurrectChunkLocationsResultDecoder    anyValueResultDecoder       = newValueResultDecoder()
	LocateSkynetShareResultDecoder          anyValueResultDecoder       = newValueResultDecoder()
	GenerateTimestampResultDecoder          anyValueResultDecoder       = newSingleValueResultDecoder("timestamp")
	GetInSyncReplicasResultDecoder          anyValueResultDecoder       = newValueResultDecoder()
	StartQueryResultDecoder                 anyValueResultDecoder       = newSingleValueResultDecoder("query_id")
	GetQueryResultDecoder                   anyValueResultDecoder       = newValueResultDecoder()
	ListQueriesResultDecoder                anyValueResultDecoder       = newValueResultDecoder()
	GetQueryResultQueryResultDecoder        anyValueResultDecoder       = newValueResultDecoder()
)
