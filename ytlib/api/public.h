#pragma once

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

// Keep in sync with NRpcProxy::NProto::EMasterReadKind.
DEFINE_ENUM(EMasterChannelKind,
    ((Leader)     (0))
    ((Follower)   (1))
    ((Cache)      (2))
);

DEFINE_ENUM(EUserWorkloadCategory,
    (Batch)
    (Interactive)
    (Realtime)
);

DEFINE_ENUM(EErrorCode,
    ((TooManyConcurrentRequests)                         (1900))
    ((JobArchiveUnavailable)                             (1910))
);

DEFINE_ENUM(ERowModificationType,
    ((Write)            (0))
    ((Delete)           (1))
    ((VersionedWrite)   (2))
);

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
struct IRowset;

using IUnversionedRowset = IRowset<NTableClient::TUnversionedRow>;
using IVersionedRowset = IRowset<NTableClient::TVersionedRow>;

DECLARE_REFCOUNTED_TYPE(IUnversionedRowset)
DECLARE_REFCOUNTED_TYPE(IVersionedRowset)
DECLARE_REFCOUNTED_STRUCT(IPersistentQueueRowset)
DECLARE_REFCOUNTED_STRUCT(TSkynetSharePartsLocations);

struct TAdminOptions;
struct TClientOptions;
struct TTransactionParticipantOptions;

struct TTimeoutOptions;
struct TTransactionalOptions;
struct TPrerequisiteOptions;
struct TMasterReadOptions;
struct TMutatingOptions;
struct TSuppressableAccessTrackingOptions;
struct TTabletRangeOptions;

struct TGetFileFromCacheResult;
struct TPutFileToCacheResult;

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IAdmin)
DECLARE_REFCOUNTED_STRUCT(IClientBase)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(ITransaction)

DECLARE_REFCOUNTED_STRUCT(INativeConnection)
DECLARE_REFCOUNTED_STRUCT(INativeClient)
DECLARE_REFCOUNTED_STRUCT(INativeTransaction)

DECLARE_REFCOUNTED_STRUCT(IProxyConnection)

DECLARE_REFCOUNTED_STRUCT(IFileWriter)

DECLARE_REFCOUNTED_STRUCT(IJournalReader)
DECLARE_REFCOUNTED_STRUCT(IJournalWriter)

DECLARE_REFCOUNTED_CLASS(TPersistentQueuePoller)

DECLARE_REFCOUNTED_CLASS(TConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TNativeConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TTableMountCacheConfig)
DECLARE_REFCOUNTED_CLASS(TFileReaderConfig)
DECLARE_REFCOUNTED_CLASS(TFileWriterConfig)
DECLARE_REFCOUNTED_CLASS(TJournalReaderConfig)
DECLARE_REFCOUNTED_CLASS(TJournalWriterConfig)
DECLARE_REFCOUNTED_CLASS(TPersistentQueuePollerConfig)

DECLARE_REFCOUNTED_STRUCT(TPrerequisiteRevisionConfig)

DECLARE_REFCOUNTED_STRUCT(TSchedulingOptions)

DECLARE_REFCOUNTED_CLASS(TJobInputReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

