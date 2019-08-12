package yterrors

import "fmt"

const (
	CodeOK                                 ErrorCode = 0
	CodeGeneric                            ErrorCode = 1
	CodeCanceled                           ErrorCode = 2
	CodeTimeout                            ErrorCode = 3
	CodeTransportError                     ErrorCode = 100
	CodeProtocolError                      ErrorCode = 101
	CodeNoSuchService                      ErrorCode = 102
	CodeNoSuchMethod                       ErrorCode = 103
	CodeUnavailable                        ErrorCode = 105
	CodePoisonPill                         ErrorCode = 106
	CodeRPCRequestQueueSizeLimitExceeded   ErrorCode = 108
	CodeRPCAuthenticationError             ErrorCode = 109
	CodeInvalidCsrfToken                   ErrorCode = 110
	CodeInvalidCredentials                 ErrorCode = 111
	CodeNoSuchOperation                    ErrorCode = 200
	CodeInvalidOperationState              ErrorCode = 201
	CodeTooManyOperations                  ErrorCode = 202
	CodeNoSuchJob                          ErrorCode = 203
	CodeOperationFailedOnJobRestart        ErrorCode = 210
	CodeSortOrderViolation                 ErrorCode = 301
	CodeInvalidDoubleValue                 ErrorCode = 302
	CodeIncomparableType                   ErrorCode = 303
	CodeUnhashableType                     ErrorCode = 304
	CodeCorruptedNameTable                 ErrorCode = 305
	CodeUniqueKeyViolation                 ErrorCode = 306
	CodeSchemaViolation                    ErrorCode = 307
	CodeRowWeightLimitExceeded             ErrorCode = 308
	CodeInvalidColumnFilter                ErrorCode = 309
	CodeInvalidColumnRenaming              ErrorCode = 310
	CodeIncompatibleKeyColumns             ErrorCode = 311
	CodeReaderDeadlineExpired              ErrorCode = 312
	CodeSameTransactionLockConflict        ErrorCode = 400
	CodeDescendantTransactionLockConflict  ErrorCode = 401
	CodeConcurrentTransactionLockConflict  ErrorCode = 402
	CodePendingLockConflict                ErrorCode = 403
	CodeLockDestroyed                      ErrorCode = 404
	CodeResolveError                       ErrorCode = 500
	CodeAlreadyExists                      ErrorCode = 501
	CodeMaxChildCountViolation             ErrorCode = 502
	CodeMaxStringLengthViolation           ErrorCode = 503
	CodeMaxAttributeSizeViolation          ErrorCode = 504
	CodeMaxKeyLengthViolation              ErrorCode = 505
	CodeNoSuchSnapshot                     ErrorCode = 600
	CodeNoSuchChangelog                    ErrorCode = 601
	CodeInvalidEpoch                       ErrorCode = 602
	CodeInvalidVersion                     ErrorCode = 603
	CodeOutOfOrderMutations                ErrorCode = 609
	CodeInvalidSnapshotVersion             ErrorCode = 610
	CodeAllTargetNodesFailed               ErrorCode = 700
	CodeSendBlocksFailed                   ErrorCode = 701
	CodeNoSuchSession                      ErrorCode = 702
	CodeSessionAlreadyExists               ErrorCode = 703
	CodeChunkAlreadyExists                 ErrorCode = 704
	CodeWindowError                        ErrorCode = 705
	CodeBlockContentMismatch               ErrorCode = 706
	CodeNoSuchBlock                        ErrorCode = 707
	CodeNoSuchChunk                        ErrorCode = 708
	CodeNoLocationAvailable                ErrorCode = 710
	CodeIOError                            ErrorCode = 711
	CodeMasterCommunicationFailed          ErrorCode = 712
	CodeNoSuchChunkTree                    ErrorCode = 713
	CodeMasterNotConnected                 ErrorCode = 714
	CodeChunkUnavailable                   ErrorCode = 716
	CodeNoSuchChunkList                    ErrorCode = 717
	CodeWriteThrottlingActive              ErrorCode = 718
	CodeNoSuchMedium                       ErrorCode = 719
	CodeOptimisticLockFailure              ErrorCode = 720
	CodeInvalidBlockChecksum               ErrorCode = 721
	CodeBlockOutOfRange                    ErrorCode = 722
	CodeObjectNotReplicated                ErrorCode = 723
	CodeMissingExtension                   ErrorCode = 724
	CodeBandwidthThrottlingFailed          ErrorCode = 725
	CodeReaderTimeout                      ErrorCode = 726
	CodeInvalidElectionState               ErrorCode = 800
	CodeInvalidLeader                      ErrorCode = 801
	CodeInvalidElectionEpoch               ErrorCode = 802
	CodeAuthenticationError                ErrorCode = 900
	CodeAuthorizationError                 ErrorCode = 901
	CodeAccountLimitExceeded               ErrorCode = 902
	CodeUserBanned                         ErrorCode = 903
	CodeRequestQueueSizeLimitExceeded      ErrorCode = 904
	CodeNoSuchAccount                      ErrorCode = 905
	CodeSafeModeEnabled                    ErrorCode = 906
	CodePrerequisiteCheckFailed            ErrorCode = 1000
	CodeConfigCreationFailed               ErrorCode = 1100
	CodeAbortByScheduler                   ErrorCode = 1101
	CodeResourceOverdraft                  ErrorCode = 1102
	CodeWaitingJobTimeout                  ErrorCode = 1103
	CodeSlotNotFound                       ErrorCode = 1104
	CodeJobEnvironmentDisabled             ErrorCode = 1105
	CodeJobProxyConnectionFailed           ErrorCode = 1106
	CodeArtifactCopyingFailed              ErrorCode = 1107
	CodeNodeDirectoryPreparationFailed     ErrorCode = 1108
	CodeSlotLocationDisabled               ErrorCode = 1109
	CodeQuotaSettingFailed                 ErrorCode = 1110
	CodeRootVolumePreparationFailed        ErrorCode = 1111
	CodeNotEnoughDiskSpace                 ErrorCode = 1112
	CodeArtifactDownloadFailed             ErrorCode = 1113
	CodeJobProxyFailed                     ErrorCode = 1120
	CodeMemoryLimitExceeded                ErrorCode = 1200
	CodeMemoryCheckFailed                  ErrorCode = 1201
	CodeJobTimeLimitExceeded               ErrorCode = 1202
	CodeUnsupportedJobType                 ErrorCode = 1203
	CodeJobNotPrepared                     ErrorCode = 1204
	CodeUserJobFailed                      ErrorCode = 1205
	CodeLocalChunkReaderFailed             ErrorCode = 1300
	CodeLayerUnpackingFailed               ErrorCode = 1301
	CodeAborted                            ErrorCode = 1500
	CodeResolveTimedOut                    ErrorCode = 1501
	CodeNoSuchNode                         ErrorCode = 1600
	CodeInvalidState                       ErrorCode = 1601
	CodeNoSuchNetwork                      ErrorCode = 1602
	CodeNoSuchRack                         ErrorCode = 1603
	CodeNoSuchDataCenter                   ErrorCode = 1604
	CodeTransactionLockConflict            ErrorCode = 1700
	CodeNoSuchTablet                       ErrorCode = 1701
	CodeTabletNotMounted                   ErrorCode = 1702
	CodeAllWritesDisabled                  ErrorCode = 1703
	CodeInvalidMountRevision               ErrorCode = 1704
	CodeTableReplicaAlreadyExists          ErrorCode = 1705
	CodeInvalidTabletState                 ErrorCode = 1706
	CodeShellExited                        ErrorCode = 1800
	CodeShellManagerShutDown               ErrorCode = 1801
	CodeTooManyConcurrentRequests          ErrorCode = 1900
	CodeJobArchiveUnavailable              ErrorCode = 1910
	CodeAPINoSuchOperation                 ErrorCode = 1915
	CodeDataSliceLimitExceeded             ErrorCode = 2000
	CodeMaxDataWeightPerJobExceeded        ErrorCode = 2001
	CodeMaxPrimaryDataWeightPerJobExceeded ErrorCode = 2002
	CodeProxyBanned                        ErrorCode = 2100
	CodeAgentCallFailed                    ErrorCode = 4400
	CodeNoOnlineNodeToScheduleJob          ErrorCode = 4410
	CodeMaterializationFailed              ErrorCode = 4415
	CodeNoSuchTransaction                  ErrorCode = 11000
	CodeFailedToStartContainer             ErrorCode = 13000
	CodeJobIsNotRunning                    ErrorCode = 17000
)

func (e ErrorCode) String() string {
	switch e {
	case CodeOK:
		return "OK"
	case CodeGeneric:
		return "Generic"
	case CodeCanceled:
		return "Canceled"
	case CodeTimeout:
		return "Timeout"
	case CodeTransportError:
		return "TransportError"
	case CodeProtocolError:
		return "ProtocolError"
	case CodeNoSuchService:
		return "NoSuchService"
	case CodeNoSuchMethod:
		return "NoSuchMethod"
	case CodeUnavailable:
		return "Unavailable"
	case CodePoisonPill:
		return "PoisonPill"
	case CodeRPCRequestQueueSizeLimitExceeded:
		return "RPCRequestQueueSizeLimitExceeded"
	case CodeRPCAuthenticationError:
		return "RPCAuthenticationError"
	case CodeInvalidCsrfToken:
		return "InvalidCsrfToken"
	case CodeInvalidCredentials:
		return "InvalidCredentials"
	case CodeNoSuchOperation:
		return "NoSuchOperation"
	case CodeInvalidOperationState:
		return "InvalidOperationState"
	case CodeTooManyOperations:
		return "TooManyOperations"
	case CodeNoSuchJob:
		return "NoSuchJob"
	case CodeOperationFailedOnJobRestart:
		return "OperationFailedOnJobRestart"
	case CodeSortOrderViolation:
		return "SortOrderViolation"
	case CodeInvalidDoubleValue:
		return "InvalidDoubleValue"
	case CodeIncomparableType:
		return "IncomparableType"
	case CodeUnhashableType:
		return "UnhashableType"
	case CodeCorruptedNameTable:
		return "CorruptedNameTable"
	case CodeUniqueKeyViolation:
		return "UniqueKeyViolation"
	case CodeSchemaViolation:
		return "SchemaViolation"
	case CodeRowWeightLimitExceeded:
		return "RowWeightLimitExceeded"
	case CodeInvalidColumnFilter:
		return "InvalidColumnFilter"
	case CodeInvalidColumnRenaming:
		return "InvalidColumnRenaming"
	case CodeIncompatibleKeyColumns:
		return "IncompatibleKeyColumns"
	case CodeReaderDeadlineExpired:
		return "ReaderDeadlineExpired"
	case CodeSameTransactionLockConflict:
		return "SameTransactionLockConflict"
	case CodeDescendantTransactionLockConflict:
		return "DescendantTransactionLockConflict"
	case CodeConcurrentTransactionLockConflict:
		return "ConcurrentTransactionLockConflict"
	case CodePendingLockConflict:
		return "PendingLockConflict"
	case CodeLockDestroyed:
		return "LockDestroyed"
	case CodeResolveError:
		return "ResolveError"
	case CodeAlreadyExists:
		return "AlreadyExists"
	case CodeMaxChildCountViolation:
		return "MaxChildCountViolation"
	case CodeMaxStringLengthViolation:
		return "MaxStringLengthViolation"
	case CodeMaxAttributeSizeViolation:
		return "MaxAttributeSizeViolation"
	case CodeMaxKeyLengthViolation:
		return "MaxKeyLengthViolation"
	case CodeNoSuchSnapshot:
		return "NoSuchSnapshot"
	case CodeNoSuchChangelog:
		return "NoSuchChangelog"
	case CodeInvalidEpoch:
		return "InvalidEpoch"
	case CodeInvalidVersion:
		return "InvalidVersion"
	case CodeOutOfOrderMutations:
		return "OutOfOrderMutations"
	case CodeInvalidSnapshotVersion:
		return "InvalidSnapshotVersion"
	case CodeAllTargetNodesFailed:
		return "AllTargetNodesFailed"
	case CodeSendBlocksFailed:
		return "SendBlocksFailed"
	case CodeNoSuchSession:
		return "NoSuchSession"
	case CodeSessionAlreadyExists:
		return "SessionAlreadyExists"
	case CodeChunkAlreadyExists:
		return "ChunkAlreadyExists"
	case CodeWindowError:
		return "WindowError"
	case CodeBlockContentMismatch:
		return "BlockContentMismatch"
	case CodeNoSuchBlock:
		return "NoSuchBlock"
	case CodeNoSuchChunk:
		return "NoSuchChunk"
	case CodeNoLocationAvailable:
		return "NoLocationAvailable"
	case CodeIOError:
		return "IOError"
	case CodeMasterCommunicationFailed:
		return "MasterCommunicationFailed"
	case CodeNoSuchChunkTree:
		return "NoSuchChunkTree"
	case CodeMasterNotConnected:
		return "MasterNotConnected"
	case CodeChunkUnavailable:
		return "ChunkUnavailable"
	case CodeNoSuchChunkList:
		return "NoSuchChunkList"
	case CodeWriteThrottlingActive:
		return "WriteThrottlingActive"
	case CodeNoSuchMedium:
		return "NoSuchMedium"
	case CodeOptimisticLockFailure:
		return "OptimisticLockFailure"
	case CodeInvalidBlockChecksum:
		return "InvalidBlockChecksum"
	case CodeBlockOutOfRange:
		return "BlockOutOfRange"
	case CodeObjectNotReplicated:
		return "ObjectNotReplicated"
	case CodeMissingExtension:
		return "MissingExtension"
	case CodeBandwidthThrottlingFailed:
		return "BandwidthThrottlingFailed"
	case CodeReaderTimeout:
		return "ReaderTimeout"
	case CodeInvalidElectionState:
		return "InvalidElectionState"
	case CodeInvalidLeader:
		return "InvalidLeader"
	case CodeInvalidElectionEpoch:
		return "InvalidElectionEpoch"
	case CodeAuthenticationError:
		return "AuthenticationError"
	case CodeAuthorizationError:
		return "AuthorizationError"
	case CodeAccountLimitExceeded:
		return "AccountLimitExceeded"
	case CodeUserBanned:
		return "UserBanned"
	case CodeRequestQueueSizeLimitExceeded:
		return "RequestQueueSizeLimitExceeded"
	case CodeNoSuchAccount:
		return "NoSuchAccount"
	case CodeSafeModeEnabled:
		return "SafeModeEnabled"
	case CodePrerequisiteCheckFailed:
		return "PrerequisiteCheckFailed"
	case CodeConfigCreationFailed:
		return "ConfigCreationFailed"
	case CodeAbortByScheduler:
		return "AbortByScheduler"
	case CodeResourceOverdraft:
		return "ResourceOverdraft"
	case CodeWaitingJobTimeout:
		return "WaitingJobTimeout"
	case CodeSlotNotFound:
		return "SlotNotFound"
	case CodeJobEnvironmentDisabled:
		return "JobEnvironmentDisabled"
	case CodeJobProxyConnectionFailed:
		return "JobProxyConnectionFailed"
	case CodeArtifactCopyingFailed:
		return "ArtifactCopyingFailed"
	case CodeNodeDirectoryPreparationFailed:
		return "NodeDirectoryPreparationFailed"
	case CodeSlotLocationDisabled:
		return "SlotLocationDisabled"
	case CodeQuotaSettingFailed:
		return "QuotaSettingFailed"
	case CodeRootVolumePreparationFailed:
		return "RootVolumePreparationFailed"
	case CodeNotEnoughDiskSpace:
		return "NotEnoughDiskSpace"
	case CodeArtifactDownloadFailed:
		return "ArtifactDownloadFailed"
	case CodeJobProxyFailed:
		return "JobProxyFailed"
	case CodeMemoryLimitExceeded:
		return "MemoryLimitExceeded"
	case CodeMemoryCheckFailed:
		return "MemoryCheckFailed"
	case CodeJobTimeLimitExceeded:
		return "JobTimeLimitExceeded"
	case CodeUnsupportedJobType:
		return "UnsupportedJobType"
	case CodeJobNotPrepared:
		return "JobNotPrepared"
	case CodeUserJobFailed:
		return "UserJobFailed"
	case CodeLocalChunkReaderFailed:
		return "LocalChunkReaderFailed"
	case CodeLayerUnpackingFailed:
		return "LayerUnpackingFailed"
	case CodeAborted:
		return "Aborted"
	case CodeResolveTimedOut:
		return "ResolveTimedOut"
	case CodeNoSuchNode:
		return "NoSuchNode"
	case CodeInvalidState:
		return "InvalidState"
	case CodeNoSuchNetwork:
		return "NoSuchNetwork"
	case CodeNoSuchRack:
		return "NoSuchRack"
	case CodeNoSuchDataCenter:
		return "NoSuchDataCenter"
	case CodeTransactionLockConflict:
		return "TransactionLockConflict"
	case CodeNoSuchTablet:
		return "NoSuchTablet"
	case CodeTabletNotMounted:
		return "TabletNotMounted"
	case CodeAllWritesDisabled:
		return "AllWritesDisabled"
	case CodeInvalidMountRevision:
		return "InvalidMountRevision"
	case CodeTableReplicaAlreadyExists:
		return "TableReplicaAlreadyExists"
	case CodeInvalidTabletState:
		return "InvalidTabletState"
	case CodeShellExited:
		return "ShellExited"
	case CodeShellManagerShutDown:
		return "ShellManagerShutDown"
	case CodeTooManyConcurrentRequests:
		return "TooManyConcurrentRequests"
	case CodeJobArchiveUnavailable:
		return "JobArchiveUnavailable"
	case CodeAPINoSuchOperation:
		return "APINoSuchOperation"
	case CodeDataSliceLimitExceeded:
		return "DataSliceLimitExceeded"
	case CodeMaxDataWeightPerJobExceeded:
		return "MaxDataWeightPerJobExceeded"
	case CodeMaxPrimaryDataWeightPerJobExceeded:
		return "MaxPrimaryDataWeightPerJobExceeded"
	case CodeProxyBanned:
		return "ProxyBanned"
	case CodeAgentCallFailed:
		return "AgentCallFailed"
	case CodeNoOnlineNodeToScheduleJob:
		return "NoOnlineNodeToScheduleJob"
	case CodeMaterializationFailed:
		return "MaterializationFailed"
	case CodeNoSuchTransaction:
		return "NoSuchTransaction"
	case CodeFailedToStartContainer:
		return "FailedToStartContainer"
	case CodeJobIsNotRunning:
		return "JobIsNotRunning"
	default:
		return fmt.Sprintf("UnknownCode%d", int(e))
	}
}
