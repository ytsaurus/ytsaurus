#pragma once

#include "wrappers.h"
#include <mapreduce/yt/library/table_schema/protobuf.h>

/** Using lambdas to perform simple YT operations.
 * ATTN! Only lambdas that are plain functions (i.e. no captured variables) are currently supported.
 * ATTN! This implementation has some performance cost compared to classic YT C++ API because
 *       lambdas here are called by pointer, not inlined.
 *
 * CopyIf            - copy records on whose the predicate is TRUE from src to dst table
 * TransformCopyIf   - transform records from src to dst (return false from lambda to drop current record)
 *                     ATTN: dst is a buffer that is NOT cleared between calls
 * AdditiveMapReduce - transform records using 1st lambda, sum them using 2nd lambda
 *                     ATTN: output is not sorted (as per MapReduce specifics)
 * AdditiveMapReduceSorted - same as AdditiveMapReduce, but the table is sorted after MR.
 *
 * example:
 * NYT::CopyIf<TMyProtoMsg>(client, inTable, outTable,
 *     [](auto& rec) { return rec.GetSomeField() > 10000; });
 *
 * example:
 * NYT::TransformCopyIf<TDataMsg, TStatsMsg>(
 *     client, inTable, outTable,
 *     [](auto& src, auto& dst) { dst.SetField(src.GetField()); return true; });
 *
 * example:
 * AdditiveMapReduce<TDataMsg, TStatsMsg>(
 *     client,
 *     inTable, // or inTables
 *     outTable,
 *     "name", // sort key, matches TStatsMsg::Name
 *     [](auto& src, auto& dst) {
 *         // dst is a buffer that may contain garbage.
 *         // We don't need to clear it because we set all fields.
 *         dst.SetName(src.GetSomeField());
 *         dst.SetCount(src.GetSomeCountField());
 *         return true;
 *     },
 *     [](auto& src, auto& dst) {
 *         // dst is initialized by the first record of equal key range.
 *         // This lambda function is called starting from the 2nd record.
 *         dst.SetCount(src.GetCount() + dst.GetCount());
 *     });
 *
 */

// ==============================================
namespace NYT {
// ==============================================

template <class T>
void CopyIf(const IClientBasePtr& client, const TRichYPath& from, const TRichYPath& to, bool (*p)(const T&)) {
    client->Map(
        TMapOperationSpec()
            .AddInput<T>(from)
            .template AddOutput<T>(WithSchema<T>(to))
            .Ordered(true),
        p ? new TCopyIfMapper<T>(p) : nullptr);
}

template <class R, class W>
void TransformCopyIf(const IClientBasePtr& client, const TRichYPath& from, const TRichYPath& to, bool (*mapper)(const R&, W&)) {
    client->Map(
        TMapOperationSpec()
            .AddInput<R>(from)
            .template AddOutput<W>(WithSchema<W>(to))
            .Ordered(true),
        mapper ? new TTransformMapper<R, W>(mapper) : nullptr);
}

template <class R, class W>
void AdditiveMapReduce(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    bool (*mapper)(const R&, W&),
    void (*reducer)(const W&, W&))
{
    auto spec = TMapReduceOperationSpec()
        .template AddOutput<W>(WithSchema<W>(to))
        .ReduceBy(reduceFields);

    for (auto& input : from.Parts_) {
        spec.template AddInput<R>(input);
    }

    client->MapReduce(
        spec,
        mapper ? new TTransformMapper<R, W>(mapper) : nullptr,
        reducer ? new TAdditiveReducer<W>(reducer) : nullptr,
        reducer ? new TAdditiveReducer<W>(reducer) : nullptr);
}

template <class R, class W>
void AdditiveMapReduceSorted(
    const IClientBasePtr& client,
    const TKeyBase<TRichYPath>& from,
    const TRichYPath& to,
    const TKeyColumns& reduceFields,
    bool (*mapper)(const R&, W&),
    void (*reducer)(const W&, W&))
{
    auto tx = client->StartTransaction();
    AdditiveMapReduce(tx, from, to, reduceFields, mapper, reducer);

    tx->Sort(
        TSortOperationSpec()
            .AddInput(to)
            .Output(to)
            .SortBy(reduceFields));
    tx->Commit();
}

// ==============================================
} // namespace NYT
// ==============================================
