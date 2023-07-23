#include "serialize.h"

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NHydra {

using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger,
    int version,
    IThreadPoolPtr backgroundThreadPool)
    : TEntityStreamSaveContext(output, version)
    , Logger_(std::move(logger))
    , CheckpointableOutput_(output)
    , BackgroundThreadPool_(std::move(backgroundThreadPool))
{ }

TSaveContext::TSaveContext(
    IZeroCopyOutput* output,
    const TSaveContext* parentContext)
    : TEntityStreamSaveContext(
        output,
        parentContext->GetVersion())
    , Logger_(parentContext->GetLogger())
{ }

const NLogging::TLogger& TSaveContext::GetLogger() const
{
    return Logger_;
}

void TSaveContext::MakeCheckpoint()
{
    Output_.FlushBuffer();
    CheckpointableOutput_->MakeCheckpoint();
}

IInvokerPtr TSaveContext::GetBackgroundInvoker() const
{
    return BackgroundThreadPool_ ? BackgroundThreadPool_->GetInvoker() : nullptr;
}

int TSaveContext::GetBackgroundParallelism() const
{
    return BackgroundThreadPool_ ? BackgroundThreadPool_->GetThreadCount() : 0;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(ICheckpointableInputStream* input)
    : TEntityStreamLoadContext(input)
    , CheckpointableInput_(input)
{ }

void TLoadContext::SkipToCheckpoint()
{
    Input_.ClearBuffer();
    CheckpointableInput_->SkipToCheckpoint();
}

i64 TLoadContext::GetOffset() const
{
    return CheckpointableInput_->GetOffset();
}

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

/*!
 *  Each mutation record has the following format:
 *  - TMutationRecordHeader
 *  - serialized NProto::TMutationHeader (of size #TMutationRecordHeader::HeaderSize)
 *  - custom mutation data (of size #TMutationRecordHeader::DataSize)
 */
struct TFixedMutationHeader
{
    i32 HeaderSize;
    i32 DataSize;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    TRef data)
{
    TFixedMutationHeader recordHeader;
    recordHeader.HeaderSize = mutationHeader.ByteSize();
    recordHeader.DataSize = data.Size();

    size_t recordSize =
        sizeof(TFixedMutationHeader) +
        recordHeader.HeaderSize +
        recordHeader.DataSize;

    struct TMutationRecordTag { };
    auto recordData = TSharedMutableRef::Allocate<TMutationRecordTag>(recordSize, {.InitializeStorage = false});
    YT_ASSERT(recordData.Size() >= recordSize);

    std::copy(
        reinterpret_cast<ui8*>(&recordHeader),
        reinterpret_cast<ui8*>(&recordHeader + 1),
        recordData.Begin());
    YT_VERIFY(mutationHeader.SerializeToArray(
        recordData.Begin() + sizeof (TFixedMutationHeader),
        recordHeader.HeaderSize));
    std::copy(
        data.Begin(),
        data.End(),
        recordData.Begin() + sizeof (TFixedMutationHeader) + recordHeader.HeaderSize);

    return recordData;
}

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData)
{
    YT_VERIFY(recordData.size() >= sizeof (TFixedMutationHeader));
    auto* recordHeader = reinterpret_cast<const TFixedMutationHeader*>(recordData.Begin());

    size_t headerStartOffset = sizeof (TFixedMutationHeader);
    size_t headerEndOffset = headerStartOffset + recordHeader->HeaderSize;
    YT_VERIFY(recordData.size() >= headerEndOffset);
    DeserializeProto(mutationHeader, recordData.Slice(headerStartOffset, headerEndOffset));

    size_t dataStartOffset = headerEndOffset;
    size_t dataEndOffset = dataStartOffset + recordHeader->DataSize;
    YT_VERIFY(recordData.size() >= dataEndOffset);
    *mutationData = recordData.Slice(dataStartOffset, dataEndOffset);
}

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_SNAPSHOT_META_FIELDS(XX) \
    XX(sequence_number, i64) \
    XX(random_seed, ui64) \
    XX(state_hash, ui64) \
    XX(timestamp, ui64) \
    XX(last_segment_id, i32) \
    XX(last_record_id, i32) \
    XX(last_mutation_term, i32)

void Serialize(
    const NProto::TSnapshotMeta& meta,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
#define XX(name, type) \
            .DoIf(meta.has_ ## name(), [&] (auto fluent) { \
                fluent.Item(#name).Value(meta.name()); \
            })
            ITERATE_SNAPSHOT_META_FIELDS(XX)
#undef XX
        .EndMap();
}

void Deserialize(
    NProto::TSnapshotMeta& meta,
    INodePtr node)
{
    if (node->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Cannot parse snapshot meta from %Qlv",
            node->GetType());
    }

    auto mapNode = node->AsMap();

#define XX(name, type) \
    if (auto child = mapNode->FindChild(#name)) { \
        meta.set_ ## name(ConvertTo<type>(child)); \
    } else { \
        meta.clear_ ## name(); \
    }
    ITERATE_SNAPSHOT_META_FIELDS(XX)
#undef XX
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
