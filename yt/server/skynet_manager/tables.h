#pragma once

#include "public.h"
#include "rb_torrent.h"

#include <yt/server/skynet_manager/resource.pb.h>

#include <yt/client/api/public.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TResourceLink
    : public NYTree::TYsonSerializable
{
    TResourceLink()
    {
        RegisterParameter("resource_id", ResourceId);
        RegisterParameter("duplicate_id", DuplicateId);
        RegisterParameter("key", Key);
    }

    TString ResourceId;
    TGuid DuplicateId;
    NTableClient::TOwningKey Key;
};

DEFINE_REFCOUNTED_TYPE(TResourceLink)

////////////////////////////////////////////////////////////////////////////////

struct TRequestKey
{
    NYPath::TYPath TablePath;
    ui64 TableRevision;
    std::vector<TString> KeyColumns;

    NTableClient::TKey ToRow(
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TRowBufferPtr& rowBuffer) const;

    void FillRow(
        NTableClient::TMutableUnversionedRow* row,
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TRowBufferPtr& rowBuffer) const;

    static TRequestKey FromRow(
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TUnversionedRow& row);
};

TString ToString(const TRequestKey& key);

void Serialize(const TRequestKey& key, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestState,
    (Creating)
    (Active)
    (Failed)
);

struct TRequestState
{
    ERequestState State;
    TGuid OwnerId;

    TInstant UpdateTime;

    TNullable<TError> Error;
    TNullable<NYson::TYsonString> Progress;
    TNullable<std::vector<TResourceLinkPtr>> Resources;

    bool IsExpired(TDuration ttl) const;
    void ValidateOwnerId(TGuid processId) const;

    static TRequestState FromRow(
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TUnversionedRow& row);
};

NTableClient::TUnversionedRow ToRow(
    const TRequestKey& key,
    const TRequestState& state,
    const NTableClient::TNameTablePtr& nameTable,
    const NTableClient::TRowBufferPtr& rowBuffer);

NTableClient::TUnversionedRow ToRow(
    const NProto::TResource& resource,
    TGuid duplicateId,
    const TResourceId& resourceId,
    const NYPath::TYPath& tablePath,
    const NTableClient::TNameTablePtr& nameTable,
    const NTableClient::TRowBufferPtr& rowBuffer,
    std::vector<NTableClient::TUnversionedRow>* filesRows);

////////////////////////////////////////////////////////////////////////////////

class TTables
    : public TRefCounted
{
public:
    TTables(
        const NApi::IClientPtr& client,
        const TClusterConnectionConfigPtr& config);

    THashSet<TResourceId> ListResources();
    std::vector<TRequestKey> ListActiveRequests();

    bool StartRequest(const TRequestKey& key, TRequestState* state);
    void UpdateStatus(
        const TRequestKey& key,
        TNullable<NYson::TYsonString> progress,
        TNullable<TError> error);
    std::vector<TResourceId> FinishRequest(const TRequestKey& key, const std::vector<TTableShard>& shards);
    void EraseRequest(const TRequestKey& key);

    void GetResource(
        const TResourceId& resourceId,
        NYPath::TYPath* tableRange,
        NProto::TResource* resource);

private:
    const TClusterConnectionConfigPtr Config_;
    const NApi::IClientPtr Client_;
    const TGuid ProcessId_;

    const NLogging::TLogger Logger;

    const TString RequestsTable_;
    const TString ResourcesTable_;
    const TString FilesTable_;

    bool LookupRequest(
        const NApi::IClientBasePtr& client,
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const TRequestKey& key,
        TRequestState* state);
    void WriteRequest(
        const NApi::ITransactionPtr& tx,
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const TRequestKey& key,
        const TRequestState& state);
    void DeleteRequest(
        const NApi::ITransactionPtr& tx,
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const TRequestKey& key);
};

DEFINE_REFCOUNTED_TYPE(TTables)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
