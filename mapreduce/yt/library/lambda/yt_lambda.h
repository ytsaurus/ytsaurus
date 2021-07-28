#pragma once

#include "wrappers.h"

/** Using lambdas to perform simple YT operations.
 * ATTN! Only lambdas that are plain functions (i.e. no captured variables) are currently supported.
 * ATTN! This implementation has some performance cost compared to classic YT C++ API because
 *       lambdas here are called by pointer, not inlined.
 *
 * Details (in Russian): https://wiki.yandex-team.ru/yt/userdoc/cppapi/lambda/
 *
 * CopyIf            - copy records on whose the predicate is TRUE from src to dst table
 *
 * TransformCopyIf   - transform records from src to dst (return false from lambda to drop current record)
 *                     ATTN: dst is a buffer that is NOT cleared between calls
 *
 * [Additive]Reduce  - reduce input using 1st lambda, then
 *                     (optionally) transform again to output format using 2nd lambda (finalizer).
 *                     Additive* variant is only for associative reducer functions (see below).
 *
 * [Additive]MapReduce[Sorted] - transform records using 1st lambda, reduce them using 2nd lambda, then
 *                     (optionally) transform again to output format using 3rd lambda (finalizer).
 *                     ATTN: as per YT MapReduce specifics, output of MR operation is not sorted.
 *                     Use *Sorted variant so that the output is sorted after MR.
 *                     Additive* variant is only for associative reducer functions (see below).
 *                     NOTE: reducer (2nd lambda) is also automatically used as combiner.
 *
 * MapReduceCombined[Sorted] - transform records using 1st lambda (mapper), reduce them first time using
 *                     2nd lambda (combiner), sum (reduce second time) them using 3rd lambda (reducer),
 *                     (optionally) transform again to output format using 4th lambda (finalizer).
 *                     NOTE: If reducer and combiner are the same associative function,
 *                     consider AdditiveMapReduce* instead.
 *
 * Note about associative reducer functions (for Additive wrappers):
 *     - src and dst must be of the same type;
 *     - basically, it should perform simple associative operations (such as addition) on all fields;
 *     - see example below
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

extern const TString SortBySep;

template <class T>
void CopyIf(const IClientBasePtr& client, const TOneOrMany<TRichYPath>& from, const TRichYPath& to, bool (*p)(const T&)) {
    client->Map(
        NDetail::PrepareMapSpec<T, T>(from, to),
        p ? new TCopyIfMapper<T>(p) : nullptr);
}

template <class R, class W>
void TransformCopyIf(const IClientBasePtr& client, const TOneOrMany<TRichYPath>& from, const TRichYPath& to, bool (*mapper)(const R&, W&)) {
    client->Map(
        NDetail::PrepareMapSpec<R, W>(from, to),
        mapper ? new TTransformMapper<R, W>(mapper) : nullptr);
}

template <class R, class W>
void AdditiveReduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    void (*reducer)(const R&, R&),
    bool (*finalizer)(const R&, W&))
{
    auto spec = NDetail::PrepareReduceSpec<R, W>(from, to, reduceFields);

    client->Reduce(
        spec,
        NDetail::ChooseReducer<TAdditiveLambdaBufReducer<R, W>>(
            reducer, finalizer, reduceFields));
}

template <class T>
void AdditiveReduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    void (*reducer)(const T&, T&))
{
    AdditiveReduce<T, T>(client, from, to, reduceFields, reducer, nullptr);
}


template <class R, class TReducerData, class W>
void Reduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    void (*reducer)(const R&, TReducerData&),
    bool (*finalizer)(const TReducerData&, W&))
{
    auto spec = NDetail::PrepareReduceSpec<R, W>(from, to, reduceFields);

    client->Reduce(
        spec,
        NDetail::ChooseReducer<TLambdaBufReducer<R, TReducerData, W>>(
            reducer, finalizer, spec.ReduceBy_));
}

template <class R, class W>
void Reduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    void (*reducer)(const R&, W&))
{
    Reduce<R, W, W>(client, from, to, reduceFields, reducer, nullptr);
}

template <class R, class TCombined, class W>
void AdditiveMapReduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TCombined&),
    void (*reducer)(const TCombined&, TCombined&),
    bool (*finalizer)(const TCombined&, W&))
{
    auto spec = NDetail::PrepareMRSpec<R, W>(from, to, reduceFields);

    client->MapReduce(
        spec,
        mapper ? new TTransformMapper<R, TCombined>(mapper) : nullptr,
        reducer ? new TAdditiveReducer<TCombined>(reducer) : nullptr,
        NDetail::ChooseReducer<TAdditiveLambdaBufReducer<TCombined, W>>(
            reducer, finalizer, spec.ReduceBy_));
}

template <class R, class W>
void AdditiveMapReduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, W&),
    void (*reducer)(const W&, W&))
{
    AdditiveMapReduce<R, W, W>(client, from, to, reduceFields, mapper, reducer, nullptr);
}

template <class R, class TCombined, class W>
void AdditiveMapReduceSorted(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TCombined&),
    void (*reducer)(const TCombined&, TCombined&),
    bool (*finalizer)(const TCombined&, W&))
{
    auto tx = client->StartTransaction();
    AdditiveMapReduce(tx, from, to, reduceFields, mapper, reducer, finalizer);

    tx->Sort(
        TSortOperationSpec()
            .AddInput(to)
            .Output(to)
            .SortBy(NDetail::GetReduceByFields(reduceFields)));
    tx->Commit();
}

template <class R, class W>
void AdditiveMapReduceSorted(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, W&),
    void (*reducer)(const W&, W&))
{
    AdditiveMapReduceSorted<R, W, W>(client, from, to, reduceFields, mapper, reducer, nullptr);
}

template <class R, class TMapped, class TReducerData, class W>
void MapReduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*reducer)(const TMapped&, TReducerData&),
    bool (*finalizer)(const TReducerData&, W&))
{
    auto spec = NDetail::PrepareMRSpec<R, W>(from, to, reduceFields);

    client->MapReduce(
        spec,
        mapper ? new TTransformMapper<R, TMapped>(mapper) : nullptr,
        NDetail::ChooseReducer<TLambdaBufReducer<TMapped, TReducerData, W>>(
            reducer, finalizer, spec.ReduceBy_));
}

template <class R, class TMapped, class W>
void MapReduce(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*reducer)(const TMapped&, W&))
{
    MapReduce<R, TMapped, W, W>(client, from, to, reduceFields, mapper, reducer, nullptr);
}

template <class R, class TMapped, class TReducerData, class W>
void MapReduceSorted(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*reducer)(const TMapped&, TReducerData&),
    bool (*finalizer)(const TReducerData&, W&))
{
    auto tx = client->StartTransaction();
    MapReduce(tx, from, to, reduceFields, mapper, reducer, finalizer);

    tx->Sort(
        TSortOperationSpec()
            .AddInput(to)
            .Output(to)
            .SortBy(NDetail::GetReduceByFields(reduceFields)));
    tx->Commit();
}

template <class R, class TMapped, class W>
void MapReduceSorted(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*reducer)(const TMapped&, W&))
{
    MapReduceSorted<R, TMapped, W, W>(client, from, to, reduceFields, mapper, reducer, nullptr);
}


template <class R, class TMapped, class TCombined, class W>
void MapReduceCombined(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*combiner)(const TMapped&, TCombined&),
    void (*reducer)(const TCombined&, TCombined&),
    bool (*finalizer)(const TCombined&, W&))
{
    auto spec = NDetail::PrepareMRSpec<R, W>(from, to, reduceFields);

    client->MapReduce(
        spec,
        mapper ? new TTransformMapper<R, TMapped>(mapper) : nullptr,
        combiner ? new TLambdaReducer<TMapped, TCombined>(combiner, spec.ReduceBy_) : nullptr,
        NDetail::ChooseReducer<TAdditiveLambdaBufReducer<TCombined, W>>(
            reducer, finalizer, spec.ReduceBy_),
        TOperationOptions().Spec(TNode()("force_reduce_combiners", true)));
}

template <class R, class TMapped, class W>
void MapReduceCombined(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*combiner)(const TMapped&, W&),
    void (*reducer)(const W&, W&))
{
    MapReduceCombined<R, TMapped, W, W>(client, from, to, reduceFields, mapper, combiner, reducer, nullptr);
}

template <class R, class TMapped, class TCombined, class W>
void MapReduceCombinedSorted(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*combiner)(const TMapped&, TCombined&),
    void (*reducer)(const TCombined&, TCombined&),
    bool (*finalizer)(const TCombined&, W&))
{
    auto tx = client->StartTransaction();
    MapReduceCombined(tx, from, to, reduceFields, mapper, combiner, reducer, finalizer);

    tx->Sort(
        TSortOperationSpec()
            .AddInput(to)
            .Output(to)
            .SortBy(NDetail::GetReduceByFields(reduceFields)));
    tx->Commit();
}

template <class R, class TMapped, class W>
void MapReduceCombinedSorted(
    const IClientBasePtr& client,
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields,
    bool (*mapper)(const R&, TMapped&),
    void (*combiner)(const TMapped&, W&),
    void (*reducer)(const W&, W&))
{
    MapReduceCombinedSorted<R, TMapped, W, W>(client, from, to, reduceFields, mapper, combiner, reducer, nullptr);
}

// ==============================================
} // namespace NYT
// ==============================================
