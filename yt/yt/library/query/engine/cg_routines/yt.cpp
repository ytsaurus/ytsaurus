#include "yt.h"

#include <yt/yt/library/query/engine_api/expression_context.h>

#include <yt/yt/library/web_assembly/api/compartment.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/coding/varint.h>
#include <library/cpp/yt/memory/chunked_memory_pool.h>
#include <library/cpp/yt/misc/guid.h>

#include <util/stream/mem.h>

#include <algorithm>
#include <optional>
#include <tuple>

namespace NYT::NQueryClient::NRoutines {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NWebAssembly;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

char* Allocate(TExpressionContext* context, size_t byteCount)
{
    return context->AllocateUnaligned(byteCount, EAddressSpace::WebAssembly);
}

struct TChunkReplica
{
    TChunkLocationIndex::TUnderlying LocationIndex;
    int ReplicaIndex;
    TNodeId::TUnderlying NodeId;
    EChunkReplicaState ReplicaState = EChunkReplicaState::Generic;

    auto operator<=>(const TChunkReplica& other) const = default;
};

auto CompareReplicasWithoutState(const TChunkReplica& lhs, const TChunkReplica& rhs)
{
    return std::tie(lhs.LocationIndex, lhs.ReplicaIndex) <=> std::tie(rhs.LocationIndex, rhs.ReplicaIndex);
}

using TChunkReplicaList = TCompactVector<TChunkReplica, TypicalReplicaCount>;

TChunkReplicaList UniteSortedReplicas(const TChunkReplicaList& storedReplicas, const TChunkReplicaList& addedReplicas)
{
    TChunkReplicaList result;
    auto it = storedReplicas.begin();
    auto jt = addedReplicas.begin();
    while (it < storedReplicas.end() || jt < addedReplicas.end()) {
        auto replicasCompareResult = std::strong_ordering::equal;
        if (it == storedReplicas.end()) {
            replicasCompareResult = std::strong_ordering::greater;
        } else if (jt == addedReplicas.end()) {
            replicasCompareResult = std::strong_ordering::less;
        } else {
            replicasCompareResult = CompareReplicasWithoutState(*it, *jt);
        }

        if (replicasCompareResult < 0) {
            result.push_back(*it);
            ++it;
        } else if (replicasCompareResult > 0) {
            result.push_back(*jt);
            ++jt;
        } else {
            // We should always choose added replicas because ReplicaState may change.
            result.push_back(*jt);
            ++it;
            ++jt;
        }
    }

    return result;
}

TChunkReplicaList FilterSortedReplicas(const TChunkReplicaList& storedReplicas, const TChunkReplicaList& removedReplicas)
{
    TChunkReplicaList result;
    auto it = storedReplicas.begin();
    auto jt = removedReplicas.begin();
    while (it < storedReplicas.end()) {
        auto replicasCompareResult = (jt == removedReplicas.end())
            ? std::strong_ordering::less
            : CompareReplicasWithoutState(*it, *jt);

        if (replicasCompareResult < 0) {
            result.push_back(*it);
            ++it;
        } else if (replicasCompareResult > 0) {
            ++jt;
        } else {
            ++it;
            ++jt;
        }
    }
    return result;
}

TYsonItem Consume(TYsonPullParserCursor* cursor, EYsonItemType type)
{
    auto current = cursor->GetCurrent();
    if (current.GetType() != type) {
        THROW_ERROR_EXCEPTION("Unexpected YSON item type: expected %Qlv, got %Qlv",
            type,
            current.GetType());
    }
    cursor->Next();
    return current;
}

TChunkReplica ParseReplica(TYsonPullParserCursor* cursor)
{
    TChunkReplica replica;

    Consume(cursor, EYsonItemType::BeginList);

    replica.LocationIndex = Consume(cursor, EYsonItemType::Uint64Value).UncheckedAsUint64();
    replica.ReplicaIndex = Consume(cursor, EYsonItemType::Int64Value).UncheckedAsInt64();
    replica.NodeId = Consume(cursor, EYsonItemType::Uint64Value).UncheckedAsUint64();

    auto current = cursor->GetCurrent();
    if (current.GetType() == EYsonItemType::Uint64Value) {
        replica.ReplicaState = NChunkClient::EChunkReplicaState(current.UncheckedAsUint64());
        cursor->Next();
    }

    Consume(cursor, EYsonItemType::EndList);

    return replica;
}

void ParseReplicasDelta(
    TUnversionedValue* delta,
    TChunkReplicaList* replicasToAdd,
    TChunkReplicaList* replicasToRemove)
{
    if (delta->Type == EValueType::Null) {
        return;
    }

    TMemoryInput input(delta->AsStringBuf());
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);

    if (!cursor.TryConsumeFragmentStart()) {
        THROW_ERROR_EXCEPTION("Error parsing YSON as list fragment");
    }

    Consume(&cursor, EYsonItemType::BeginList);
    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        replicasToAdd->push_back(ParseReplica(cursor));
    });
    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        replicasToRemove->push_back(ParseReplica(cursor));
    });
    Consume(&cursor, EYsonItemType::EndList);
}

void ParseReplicas(TUnversionedValue* value, TChunkReplicaList* replicas)
{
    if (value->Type == EValueType::Null) {
        return;
    }

    TMemoryInput input(value->AsStringBuf());
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);

    if (!cursor.TryConsumeFragmentStart()) {
        THROW_ERROR_EXCEPTION("Error parsing yson as list fragment");
    }

    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        replicas->push_back(ParseReplica(cursor));
    });
}

void DumpReplicas(IYsonConsumer* consumer, const TChunkReplicaList& replicas)
{
    BuildYsonFluently(consumer)
        .DoListFor(replicas, [] (TFluentList fluent, const TChunkReplica& replica) {
            auto replicaFluent = fluent
                .Item()
                .BeginList()
                    .Item().Value(replica.LocationIndex)
                    .Item().Value(replica.ReplicaIndex)
                    .Item().Value(replica.NodeId);

            if (replica.ReplicaState != EChunkReplicaState::Generic) {
                replicaFluent.Item().Value(static_cast<ui8>(replica.ReplicaState));
            }
            replicaFluent.EndList();
        });
}

size_t EstimateReplicasYsonLength(const TChunkReplicaList& replicas)
{
    return (MaxGuidStringSize + 3 * MaxVarInt64Size + 10) * replicas.size() + 10;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void StoredReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    if (state1->Type == EValueType::Null) {
        *result = *state2;
        return;
    }

    TChunkReplicaList replicasToAdd1;
    TChunkReplicaList replicasToRemove1;
    ParseReplicasDelta(state1, &replicasToAdd1, &replicasToRemove1);

    TChunkReplicaList replicasToAdd2;
    TChunkReplicaList replicasToRemove2;
    ParseReplicasDelta(state2, &replicasToAdd2, &replicasToRemove2);

    std::ranges::sort(replicasToAdd1);
    std::ranges::sort(replicasToAdd2);
    std::ranges::sort(replicasToRemove2);
    auto replicasToAdd = FilterSortedReplicas(UniteSortedReplicas(replicasToAdd1, replicasToAdd2), replicasToRemove2);

    auto bufferSize = EstimateReplicasYsonLength(replicasToAdd);
    char* outputBuffer = Allocate(context, bufferSize);

    TMemoryOutput output(outputBuffer, bufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);

    BuildYsonFluently(&writer)
        .BeginList()
            .Item().Do([&] (auto fluent) {
                DumpReplicas(fluent.GetConsumer(), replicasToAdd);
            })
            .Item().Do([] (auto fluent) {
                // Replicas to remove is empty list.
                DumpReplicas(fluent.GetConsumer(), {});
            })
        .EndList();

    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

void StoredReplicaSetFinalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    TChunkReplicaList replicasToAdd;
    TChunkReplicaList replicasToRemove;

    ParseReplicasDelta(state, &replicasToAdd, &replicasToRemove);

    auto outputBufferSize = EstimateReplicasYsonLength(replicasToAdd);
    char* outputBuffer = Allocate(context, outputBufferSize);

    TMemoryOutput output(outputBuffer, outputBufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);

    DumpReplicas(&writer, replicasToAdd);

    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

void LastSeenReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    constexpr int MaxLastSeenReplicas = 3;

    if (state1->Type == EValueType::Null) {
        *result = *state2;
        return;
    }

    TChunkReplicaList lastSeenReplicas;
    ParseReplicas(state1, &lastSeenReplicas);

    TChunkReplicaList newReplicas;
    ParseReplicas(state2, &newReplicas);
    for (const auto& replica : newReplicas) {
        // Linear complexity should be fine.
        auto it = std::find(lastSeenReplicas.begin(), lastSeenReplicas.end(), replica);
        if (it != lastSeenReplicas.end()) {
            lastSeenReplicas.erase(it);
        }
        lastSeenReplicas.push_back(replica);
    }

    std::optional<bool> isErasure;
    for (const auto& replica : lastSeenReplicas) {
        auto isReplicaErasure = replica.ReplicaIndex < GenericChunkReplicaIndex;
        if (!isErasure.has_value()) {
            isErasure = isReplicaErasure;
        }
        if (isErasure != isReplicaErasure) {
            THROW_ERROR_EXCEPTION("Erasure replicas are mixed with non-erasure");
        }
    }

    if (isErasure && *isErasure) {
        TCompactVector<std::optional<TChunkReplica>, GenericChunkReplicaIndex> erasureLastSeenReplicas;
        erasureLastSeenReplicas.resize(GenericChunkReplicaIndex);
        for (const auto& replica : lastSeenReplicas) {
            YT_VERIFY(replica.ReplicaIndex < std::ssize(erasureLastSeenReplicas));
            erasureLastSeenReplicas[replica.ReplicaIndex] = replica;
        }

        lastSeenReplicas.clear();
        for (const auto& replica : erasureLastSeenReplicas) {
            if (replica.has_value()) {
                lastSeenReplicas.push_back(*replica);
            }
        }
    } else if (std::ssize(lastSeenReplicas) > MaxLastSeenReplicas) {
        auto excessReplicaCount = std::ssize(lastSeenReplicas) - MaxLastSeenReplicas;
        lastSeenReplicas.erase(lastSeenReplicas.begin(), lastSeenReplicas.begin() + excessReplicaCount);
    }

    auto bufferSize = EstimateReplicasYsonLength(lastSeenReplicas);
    char* outputBuffer = Allocate(context, bufferSize);

    TMemoryOutput output(outputBuffer, bufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);
    DumpReplicas(&writer, lastSeenReplicas);
    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

void LastSeenReplicaSetFinalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    // The merge step already emits the final flat replica list, so finalize is a passthrough.
    *result = *state;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
