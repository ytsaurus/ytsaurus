#pragma once

#include "public.h"
#include "helpers.h"
#include "table_descriptor.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/api/table_client.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

// NB: All instances ot this class has to have static lifetime.
struct ISequoiaTransactionActionSequencer
{
    //! Returns priority of a given tx action.
    virtual int GetActionPriority(TStringBuf actionType) const = 0;

    virtual ~ISequoiaTransactionActionSequencer() = default;
};

struct TSequoiaTransactionRequestPriorities
{
    int DatalessLockRow = 0;
    int LockRow = 0;
    int WriteRow = 0;
    int DeleteRow = 0;
};

struct TSequoiaTransactionSequencingOptions
{
    const ISequoiaTransactionActionSequencer* TransactionActionSequencer = nullptr;
    std::optional<TSequoiaTransactionRequestPriorities> RequestPriorities;
};

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaClient
    : public TRefCounted
{
    virtual TFuture<NApi::TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;

    template <class TRecordKey>
    TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> LookupRows(
        const std::vector<TRecordKey>& keys,
        const NTableClient::TColumnFilter& columnFilter = {},
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;
    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TSequoiaTablePathDescriptor& tableDescriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;

    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);
    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(
        NObjectClient::TCellTag masterCellTag,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);

    virtual TFuture<void> TrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount) = 0;

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        ESequoiaTransactionType type,
        const NApi::TTransactionStartOptions& options = {},
        const std::vector<NObjectClient::TTransactionId>& cypressPrerequisiteTransactionIds = {},
        const TSequoiaTransactionSequencingOptions& sequencingOptions = {}) = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaClient)

////////////////////////////////////////////////////////////////////////////////

ISequoiaClientPtr CreateSequoiaClient(
    NApi::NNative::TSequoiaConnectionConfigPtr config,
    NApi::NNative::IClientPtr nativeClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define CLIENT_INL_H_
#include "client-inl.h"
#undef CLIENT_INL_H_
