package internal

type resultDecoder func(res *CallResult) error
type valueResultDecoder[T any] func(value T) resultDecoder
type AnyValueResultDecoder = valueResultDecoder[any]

func newSingleValueResultDecoder(key string) AnyValueResultDecoder {
	return func(value any) resultDecoder {
		return func(res *CallResult) error {
			return res.decodeSingle(key, value)
		}
	}
}

func newValueResultDecoder() AnyValueResultDecoder {
	return func(value any) resultDecoder {
		return func(res *CallResult) error {
			return res.decode(value)
		}
	}
}

func newJSONValueResultDecoder() AnyValueResultDecoder {
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
	CreateNodeResultDecoder   AnyValueResultDecoder = newSingleValueResultDecoder("node_id")
	CreateObjectResultDecoder AnyValueResultDecoder = newSingleValueResultDecoder("object_id")
	NodeExistsResultDecoder   AnyValueResultDecoder = newSingleValueResultDecoder("value")
	GetNodeResultDecoder      AnyValueResultDecoder = newSingleValueResultDecoder("value")
	ListNodeResultDecoder     AnyValueResultDecoder = newSingleValueResultDecoder("value")
	CopyMoveNodeResultDecoder AnyValueResultDecoder = newSingleValueResultDecoder("node_id")
	LinkNodeResultDecoder     AnyValueResultDecoder = newSingleValueResultDecoder("node_id")
	LockNodeResultDecoder     AnyValueResultDecoder = newValueResultDecoder()
	// The whoami method returns JSON value, not YSON, so we need to specify the decoding.
	WhoAmIResultDecoder                     AnyValueResultDecoder       = newJSONValueResultDecoder()
	IssueTokenResultDecoder                 AnyValueResultDecoder       = newValueResultDecoder()
	ListUserTokensResultDecoder             AnyValueResultDecoder       = newValueResultDecoder()
	BuildMasterSnapshotsResultDecoder       AnyValueResultDecoder       = newValueResultDecoder()
	BuildSnapshotResultDecoder              AnyValueResultDecoder       = newValueResultDecoder()
	AddMaintenanceResultDecoder             AnyValueResultDecoder       = newValueResultDecoder()
	RemoveMaintenanceResultDecoder          AnyValueResultDecoder       = newValueResultDecoder()
	CheckPermissionResultDecoder            AnyValueResultDecoder       = newValueResultDecoder()
	StartTxResultDecoder                    AnyValueResultDecoder       = newSingleValueResultDecoder("transaction_id")
	StartTabletTxResultDecoder              AnyValueResultDecoder       = newSingleValueResultDecoder("transaction_id")
	StartOperationResultDecoder             AnyValueResultDecoder       = newSingleValueResultDecoder("operation_id")
	GetOperationResultDecoder               AnyValueResultDecoder       = newValueResultDecoder()
	GetOperationByAliasResultDecoder        AnyValueResultDecoder       = newValueResultDecoder()
	ListOperationsResultDecoder             AnyValueResultDecoder       = newValueResultDecoder()
	ListJobsResultDecoder                   AnyValueResultDecoder       = newValueResultDecoder()
	GetJobStderrResultDecoder               valueResultDecoder[*[]byte] = newRawValueResultDecoder()
	PutFileToCacheResultDecoder             AnyValueResultDecoder       = newValueResultDecoder()
	GetFileFromCacheResultDecoder           AnyValueResultDecoder       = newValueResultDecoder()
	PushQueueProducerResultDecoder          AnyValueResultDecoder       = newValueResultDecoder()
	PushQueueProducerBatchResultDecoder     AnyValueResultDecoder       = newValueResultDecoder()
	CreateQueueProducerSessionResultDecoder AnyValueResultDecoder       = newValueResultDecoder()
	DisableChunkLocationsResultDecoder      AnyValueResultDecoder       = newValueResultDecoder()
	DestroyChunkLocationsResultDecoder      AnyValueResultDecoder       = newValueResultDecoder()
	ResurrectChunkLocationsResultDecoder    AnyValueResultDecoder       = newValueResultDecoder()
	LocateSkynetShareResultDecoder          AnyValueResultDecoder       = newValueResultDecoder()
	GenerateTimestampResultDecoder          AnyValueResultDecoder       = newSingleValueResultDecoder("timestamp")
	GetInSyncReplicasResultDecoder          AnyValueResultDecoder       = newValueResultDecoder()
	StartQueryResultDecoder                 AnyValueResultDecoder       = newSingleValueResultDecoder("query_id")
	GetQueryResultDecoder                   AnyValueResultDecoder       = newValueResultDecoder()
	ListQueriesResultDecoder                AnyValueResultDecoder       = newValueResultDecoder()
	GetQueryResultQueryResultDecoder        AnyValueResultDecoder       = newValueResultDecoder()
)
