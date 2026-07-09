#pragma once

#include "payload_converter.h"
#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/core/misc/atomic_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TStreamSpecs
    : public TRefCounted
{
public:
    struct TStreamIdAndSchema
    {
        TStreamId StreamId;
        NTableClient::TTableSchemaPtr Schema;
    };

    //! Constructs TStreamSpecs from a map of stream specifications.
    /*!
     *  \param streamSpecs A map where keys are TStreamId and values are maps of TStreamSpecId to stream spec pointers.
     *
     *  Expected invariants:
     *  - Each stream must have at least one spec (non-empty inner map)
     *  - All TStreamSpecId values must be unique across all streams and versions.
     *  - All stream spec schema pointers must be unique across all streams and versions.
     */
    explicit TStreamSpecs(
        const THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>& streamSpecs);

    TStreamSpecPtr GetSpec(TStreamSpecId specId) const;
    TStreamSpecPtr GetSpec(const TStreamId& streamId) const;

    NTableClient::TTableSchemaPtr GetSchema(TStreamSpecId specId) const;
    NTableClient::TTableSchemaPtr GetSchema(const TStreamId& streamId) const;

    TStreamSpecId GetLastSpecId(const TStreamId& streamId) const;

    TStreamSpecId GetStreamSpecId(const NTableClient::TTableSchemaPtr& schema) const;

    const TStreamId& GetStreamId(TStreamSpecId specId) const;

    TStreamIdAndSchema GetStreamIdAndSchema(TStreamSpecId specId) const;

private:
    struct TStreamInfo
    {
        TStreamId StreamId;
        TStreamSpecPtr Spec;
    };

private:
    THashMap<TStreamSpecId, TStreamInfo> Specs_;
    THashMap<TStreamId, TStreamSpecId> LastStreamSpecIds_;
    THashMap<NTableClient::TTableSchemaPtr, TStreamSpecId> SchemaToStreamSpecId_;
};

DEFINE_REFCOUNTED_TYPE(TStreamSpecs);

////////////////////////////////////////////////////////////////////////////////

class TComputationStreamSpecStorage
    : public TRefCounted
{
public:
    TComputationStreamSpecStorage(
        TStreamSpecsPtr streamSpecs,
        NTableClient::TTableSchemaPtr groupBySchema,
        IPayloadConverterCachePtr converterCache);

    TStreamSpecPtr GetSpec(TStreamSpecId specId) const;
    TStreamSpecPtr GetSpec(const TStreamId& streamId) const;

    NTableClient::TTableSchemaPtr GetSchema(TStreamSpecId specId) const;
    NTableClient::TTableSchemaPtr GetSchema(const TStreamId& streamId) const;

    TStreamSpecId GetLastSpecId(const TStreamId& streamId) const;

    const NTableClient::TTableSchemaPtr& GetGroupBySchema() const;

    TKey ComputeKey(const TMessage& message) const;

    TStreamSpecsPtr GetStreamSpecs() const;

private:
    const TStreamSpecsPtr StreamSpecs_;
    const NTableClient::TTableSchemaPtr GroupBySchema_;
    const IPayloadConverterCachePtr ConverterCache_;
};

DEFINE_REFCOUNTED_TYPE(TComputationStreamSpecStorage);

////////////////////////////////////////////////////////////////////////////////

class TStreamSpecStorage
    : public TRefCounted
{
public:
    explicit TStreamSpecStorage(IPayloadConverterCachePtr converterCache);

    TStreamSpecStorage(
        const TVersionedStreamSpecStorageStatePtr& versionedStorageState,
        IPayloadConverterCachePtr converterCache);

    void Reconfigure(const TVersionedStreamSpecStorageStatePtr& versionedStorageState);

    TVersion GetVersion() const;
    TStreamSpecsPtr GetStreamSpecs() const;

    TComputationStreamSpecStoragePtr GetComputationStreamSpecStorage(const TComputationId& computationId) const;

private:
    struct TSnapshot final
    {
        static constexpr bool EnableHazard = true;

        TVersion Version{TVersion(-1)};
        TStreamSpecsPtr StreamSpecs;
        THashMap<TComputationId, TComputationStreamSpecStoragePtr> ComputationStorages;
    };

private:
    const IPayloadConverterCachePtr ConverterCache_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, WriterLock_);
    TAtomicPtr<TSnapshot, /*EnableAcquireHazard*/ true> Snapshot_;
};

DEFINE_REFCOUNTED_TYPE(TStreamSpecStorage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
