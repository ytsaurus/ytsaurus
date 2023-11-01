#pragma once

#include "public.h"
#include "checkpointable_stream.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public TEntityStreamSaveContext
{
public:
    explicit TSaveContext(
        ICheckpointableOutputStream* output,
        NLogging::TLogger logger = {},
        int version = 0,
        NConcurrency::IThreadPoolPtr backgroundThreadPool = nullptr);

    TSaveContext(
        IZeroCopyOutput* output,
        const TSaveContext* parentContext);

    const NLogging::TLogger& GetLogger() const;

    void MakeCheckpoint();
    void Flush();

    IInvokerPtr GetBackgroundInvoker() const;
    int GetBackgroundParallelism() const;

private:
    const NLogging::TLogger Logger_;
    ICheckpointableOutputStream* const CheckpointableOutput_ = nullptr;
    const NConcurrency::IThreadPoolPtr BackgroundThreadPool_;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public TEntityStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(i64, LowerWriteCountDumpLimit);
    DEFINE_BYVAL_RW_PROPERTY(i64, UpperWriteCountDumpLimit);

public:
    explicit TLoadContext(
        ICheckpointableInputStream* input,
        NConcurrency::IThreadPoolPtr backgroundThreadPool = nullptr);

    TLoadContext(
        IZeroCopyInput* input,
        const TLoadContext* parentContext);

    void SkipToCheckpoint();
    i64 GetOffset() const;

    IInvokerPtr GetBackgroundInvoker() const;
    int GetBackgroundParallelism() const;

private:
    ICheckpointableInputStream* const CheckpointableInput_ = nullptr;
    const NConcurrency::IThreadPoolPtr BackgroundThreadPool_;
};

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    TRef mutationData);

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData);

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const NProto::TSnapshotMeta& meta,
    NYson::IYsonConsumer* consumer);
void Deserialize(
    NProto::TSnapshotMeta& meta,
    NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

