#include "stream_spec_storage.h"

#include "message.h"
#include "payload_converter.h"
#include "schema.h"
#include "spec.h"
#include "stream_spec_storage_state.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {

using namespace NQueryClient;
using namespace NTableClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TMap, class TKey>
const auto& GetOrThrow(const TMap& map, const TKey& key, const char* errorPrefix, const char* keyName = "Key")
{
    auto it = map.find(key);
    THROW_ERROR_EXCEPTION_IF(it == map.end(), "%v (%v: %Qv)", errorPrefix, keyName, key);
    return it->second;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStreamSpecs::TStreamSpecs(
    const THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>& streamSpecs)
{
    for (const auto& [streamId, versions] : streamSpecs) {
        YT_VERIFY(!versions.empty());

        LastStreamSpecIds_[streamId] = versions.rbegin()->first;

        for (const auto& [specId, spec] : versions) {
            {
                auto [it, inserted] = Specs_.try_emplace(specId, streamId, spec);
                THROW_ERROR_EXCEPTION_IF(!inserted,
                    "Found duplicating StreamSpecId (StreamSpecId: %v, FirstStreamId: %v, SecondStreamId: %v)",
                    specId,
                    streamId,
                    it->second.StreamId);
            }

            {
                auto [it, inserted] = SchemaToStreamSpecId_.try_emplace(spec->Schema, specId);
                THROW_ERROR_EXCEPTION_IF(!inserted,
                    "Found two StreamSpec versions with same schema (FirstStreamId: %v, SecondStreamId: %v FirstStreamSpecId: %v, SecondStreamSpecId: %v, Schema: %v)",
                    streamId,
                    GetOrDefault(Specs_, it->second, {.StreamId = {}}).StreamId,
                    specId,
                    it->second,
                    spec->Schema);
            }
        }
    }
}

TStreamSpecPtr TStreamSpecs::GetSpec(TStreamSpecId specId) const
{
    return GetOrThrow(Specs_, specId, "Unknown stream spec id", "StreamSpecId").Spec;
}

TStreamSpecPtr TStreamSpecs::GetSpec(const TStreamId& streamId) const
{
    return GetSpec(GetLastSpecId(streamId));
}

NTableClient::TTableSchemaPtr TStreamSpecs::GetSchema(TStreamSpecId specId) const
{
    return GetSpec(specId)->Schema;
}

NTableClient::TTableSchemaPtr TStreamSpecs::GetSchema(const TStreamId& streamId) const
{
    return GetSpec(streamId)->Schema;
}

TStreamSpecId TStreamSpecs::GetLastSpecId(const TStreamId& streamId) const
{
    return GetOrThrow(LastStreamSpecIds_, streamId, "Unregistered stream id", "StreamId");
}

TStreamSpecId TStreamSpecs::GetStreamSpecId(const TTableSchemaPtr& schema) const
{
    return GetOrThrow(
        SchemaToStreamSpecId_,
        schema,
        "Unregistered pointer to schema",
        "SchemaValue");
}

const TStreamId& TStreamSpecs::GetStreamId(TStreamSpecId specId) const
{
    return GetOrThrow(Specs_, specId, "Unknown stream spec id", "StreamSpecId").StreamId;
}

TStreamSpecs::TStreamIdAndSchema TStreamSpecs::GetStreamIdAndSchema(TStreamSpecId specId) const
{
    const auto& info = GetOrThrow(Specs_, specId, "Unknown stream spec id", "StreamSpecId");
    return {info.StreamId, info.Spec->Schema};
}

////////////////////////////////////////////////////////////////////////////////

TComputationStreamSpecStorage::TComputationStreamSpecStorage(
    TStreamSpecsPtr streamSpecs,
    TTableSchemaPtr groupBySchema,
    IPayloadConverterCachePtr converterCache)
    : StreamSpecs_(std::move(streamSpecs))
    , GroupBySchema_(std::move(groupBySchema))
    , ConverterCache_(std::move(converterCache))
{ }

TStreamSpecPtr TComputationStreamSpecStorage::GetSpec(TStreamSpecId specId) const
{
    return StreamSpecs_->GetSpec(specId);
}

TStreamSpecPtr TComputationStreamSpecStorage::GetSpec(const TStreamId& streamId) const
{
    return StreamSpecs_->GetSpec(streamId);
}

NTableClient::TTableSchemaPtr TComputationStreamSpecStorage::GetSchema(TStreamSpecId specId) const
{
    return StreamSpecs_->GetSchema(specId);
}

NTableClient::TTableSchemaPtr TComputationStreamSpecStorage::GetSchema(const TStreamId& streamId) const
{
    return StreamSpecs_->GetSchema(streamId);
}

TStreamSpecId TComputationStreamSpecStorage::GetLastSpecId(const TStreamId& streamId) const
{
    return StreamSpecs_->GetLastSpecId(streamId);
}

const NTableClient::TTableSchemaPtr& TComputationStreamSpecStorage::GetGroupBySchema() const
{
    return GroupBySchema_;
}

TKey TComputationStreamSpecStorage::ComputeKey(const TMessage& message) const
{
    YT_VERIFY(ConverterCache_);

    auto key = ConvertPayloadToNewSchema(
        message.Payload,
        message.PayloadSchema,
        GroupBySchema_,
        ConverterCache_);

    return TKey(std::move(key).Underlying());
}

TStreamSpecsPtr TComputationStreamSpecStorage::GetStreamSpecs() const
{
    return StreamSpecs_;
}

////////////////////////////////////////////////////////////////////////////////

TStreamSpecStorage::TStreamSpecStorage(IPayloadConverterCachePtr converterCache)
    : ConverterCache_(std::move(converterCache))
    , Snapshot_(New<TSnapshot>())
{ }

TStreamSpecStorage::TStreamSpecStorage(
    const TVersionedStreamSpecStorageStatePtr& versionedStorageState,
    IPayloadConverterCachePtr converterCache)
    : TStreamSpecStorage(std::move(converterCache))
{
    Reconfigure(versionedStorageState);
}

void TStreamSpecStorage::Reconfigure(const TVersionedStreamSpecStorageStatePtr& versionedStorageState)
{
    const auto guard = Guard(WriterLock_);

    const auto& storageState = versionedStorageState->GetValue();

    auto next = New<TSnapshot>();
    next->Version = versionedStorageState->GetVersion();
    next->StreamSpecs = New<TStreamSpecs>(storageState->StreamSpecs);

    for (const auto& [computationId, groupBySchema] : storageState->GroupBySchemas) {
        next->ComputationStorages[computationId] = New<TComputationStreamSpecStorage>(
            next->StreamSpecs,
            groupBySchema,
            ConverterCache_);
    }

    Snapshot_.Store(std::move(next));
}

TVersion TStreamSpecStorage::GetVersion() const
{
    return Snapshot_.AcquireHazard()->Version;
}

TStreamSpecsPtr TStreamSpecStorage::GetStreamSpecs() const
{
    return Snapshot_.AcquireHazard()->StreamSpecs;
}

TComputationStreamSpecStoragePtr TStreamSpecStorage::GetComputationStreamSpecStorage(
    const TComputationId& computationId) const
{
    auto snapshot = Snapshot_.AcquireHazard();
    return GetOrThrow(snapshot->ComputationStorages, computationId, "Unregistered computation", "ComputationId");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
