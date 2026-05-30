#include "inferrum.h"

#include <yt/yt/library/query/engine_api/expression_context.h>

#include <yt/yt/library/web_assembly/api/compartment.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/coding/varint.h>
#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <util/stream/mem.h>

#include <algorithm>

namespace NYT::NQueryClient::NRoutines {

using namespace NChunkClient;
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

using TReplica = TInferrumKVCacheReplica;
using TReplicaList = TCompactVector<TReplica, TypicalReplicaCount>;

void ParseInferrumKVCacheReplicaList(TYsonPullParserCursor* cursor, TReplicaList* replicas)
{
    cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
        replicas->emplace_back(Consume(cursor, EYsonItemType::Uint64Value).UncheckedAsUint64());
    });
}

void ParseInferrumKVCacheReplicasDelta(
    TUnversionedValue* delta,
    TReplicaList* replicasToAdd,
    TReplicaList* replicasToRemove)
{
    if (delta->Type == EValueType::Null) {
        return;
    }

    TMemoryInput input(delta->Data.String, delta->Length);
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);

    if (!cursor.TryConsumeFragmentStart()) {
        THROW_ERROR_EXCEPTION("Error parsing YSON as list fragment");
    }

    Consume(&cursor, EYsonItemType::BeginList);
    ParseInferrumKVCacheReplicaList(&cursor, replicasToAdd);
    ParseInferrumKVCacheReplicaList(&cursor, replicasToRemove);
    Consume(&cursor, EYsonItemType::EndList);
}

TReplicaList UniteSortedInferrumKVCacheReplicas(
    const TReplicaList& storedReplicas,
    const TReplicaList& addedReplicas)
{
    TReplicaList result;
    auto it = storedReplicas.begin();
    auto jt = addedReplicas.begin();
    while (it < storedReplicas.end() || jt < addedReplicas.end()) {
        if (jt == addedReplicas.end()) {
            result.push_back(*it++);
        } else if (it == storedReplicas.end() || *jt < *it) {
            result.push_back(*jt++);
        } else if (*it < *jt) {
            result.push_back(*it++);
        } else {
            result.push_back(*it++);
            ++jt;
        }
    }
    return result;
}

TReplicaList FilterSortedInferrumKVCacheReplicas(
    const TReplicaList& storedReplicas,
    const TReplicaList& removedReplicas)
{
    TReplicaList result;
    auto it = storedReplicas.begin();
    auto jt = removedReplicas.begin();
    while (it < storedReplicas.end()) {
        if (jt == removedReplicas.end() || *it < *jt) {
            result.push_back(*it++);
        } else if (*jt < *it) {
            ++jt;
        } else {
            ++it;
            ++jt;
        }
    }
    return result;
}

void DumpInferrumKVCacheReplicas(IYsonConsumer* consumer, const TReplicaList& replicas)
{
    BuildYsonFluently(consumer)
        .Value(replicas);
}

size_t EstimateInferrumKVCacheReplicasYsonLength(const TReplicaList& replicas)
{
    return (MaxVarUint32Size + 4) * replicas.size() + 10;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InferrumKVCacheReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    if (state1->Type == EValueType::Null) {
        *result = *state2;
        return;
    }

    TReplicaList replicasToAdd1;
    TReplicaList replicasToRemove1;
    ParseInferrumKVCacheReplicasDelta(state1, &replicasToAdd1, &replicasToRemove1);

    TReplicaList replicasToAdd2;
    TReplicaList replicasToRemove2;
    ParseInferrumKVCacheReplicasDelta(state2, &replicasToAdd2, &replicasToRemove2);

    std::ranges::sort(replicasToAdd1);
    std::ranges::sort(replicasToAdd2);
    std::ranges::sort(replicasToRemove2);
    auto replicasToAdd = FilterSortedInferrumKVCacheReplicas(
        UniteSortedInferrumKVCacheReplicas(replicasToAdd1, replicasToAdd2),
        replicasToRemove2);

    auto bufferSize = EstimateInferrumKVCacheReplicasYsonLength(replicasToAdd);
    char* outputBuffer = Allocate(context, bufferSize);

    TMemoryOutput output(outputBuffer, bufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);

    BuildYsonFluently(&writer)
        .BeginList()
            .Item().Do([&] (auto fluent) {
                DumpInferrumKVCacheReplicas(fluent.GetConsumer(), replicasToAdd);
            })
            .Item().Do([] (auto fluent) {
                DumpInferrumKVCacheReplicas(fluent.GetConsumer(), {});
            })
        .EndList();

    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

void InferrumKVCacheReplicaSetFinalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    TReplicaList replicasToAdd;
    TReplicaList replicasToRemove;

    ParseInferrumKVCacheReplicasDelta(state, &replicasToAdd, &replicasToRemove);

    auto outputBufferSize = EstimateInferrumKVCacheReplicasYsonLength(replicasToAdd);
    char* outputBuffer = Allocate(context, outputBufferSize);

    TMemoryOutput output(outputBuffer, outputBufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);

    DumpInferrumKVCacheReplicas(&writer, replicasToAdd);

    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
