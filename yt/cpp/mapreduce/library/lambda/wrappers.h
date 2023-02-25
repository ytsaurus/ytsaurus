#pragma once

#include "field_copier.h"
#include "global_saveable.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/library/table_schema/protobuf.h>
#include <util/ysaveload.h>

// ==============================================
namespace NYT::NDetail {
// ==============================================

static constexpr TStringBuf SortBySep; // empty string at the moment

template <template<class Rd, class Wr> class Op, class R, class W, class F>
class TLambdaOpBase : public Op<TTableReader<R>, TTableWriter<W>> {
public:
    TLambdaOpBase() { }
    TLambdaOpBase(F func) : Func(func) { }

    void Save(IOutputStream& stream) const override {
        // Note that this does not work with any lambda,
        // actually Func is a function pointer for now.
        // Scary raw pointer I/O will be removed when C++20 comes.
        SavePodType(&stream, Func);
        TSaveablesRegistry::Get()->SaveAll(stream);
    }
    void Load(IInputStream& stream) override {
        LoadPodType(&stream, Func);
        TSaveablesRegistry::Get()->LoadAll(stream);
    }

protected:
    F Func;
};

/**
 * Saves & Loads FieldCopier object for current reduceBy fields
 */
template <template<class Rd, class Wr> class Op, class R, class W, class F>
class TByColumnAwareLambdaOpBase : public TLambdaOpBase<Op, R, W, F> {
public:
    TByColumnAwareLambdaOpBase() : FieldCopier(TSortColumns()) { }
    TByColumnAwareLambdaOpBase(F func, const TSortColumns& reduceColumns)
        : TLambdaOpBase<Op, R, W, F>(func)
        , ReduceColumns(reduceColumns)
        , FieldCopier(reduceColumns)
    {
    }

    void Save(IOutputStream& stream) const override {
        TLambdaOpBase<Op, R, W, F>::Save(stream);
        ::Save(&stream, ReduceColumns.Parts_);
    }

    void Load(IInputStream& stream) override {
        TLambdaOpBase<Op, R, W, F>::Load(stream);
        ::Load(&stream, ReduceColumns.Parts_);
        FieldCopier = TFieldCopier<R, W>(ReduceColumns);
    }

protected:
    TSortColumns ReduceColumns;
    TFieldCopier<R, W> FieldCopier;
};

// fields before first SortBySep
TSortColumns GetReduceByFields(const TSortColumns& reduceFields);

// (all) SortBySep field(s) removed
TSortColumns GetSortByFields(const TSortColumns& reduceFields);

template<class R, class W>
TMapOperationSpec PrepareMapSpec(
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to)
{
    auto spec = TMapOperationSpec()
        .template AddOutput<W>(MaybeWithSchema<W>(to))
        .Ordered(true);

    for (auto& input : from.Parts_)
        spec.template AddInput<R>(input);
    return spec;
}

template<class R, class W>
TMapReduceOperationSpec PrepareMRSpec(
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields)
{
    // SortBySep element separates ReduceBy fields from the rest of SortBy fields
    bool hasDelim = std::find(reduceFields.Parts_.begin(), reduceFields.Parts_.end(), SortBySep) != reduceFields.Parts_.end();

    auto spec = TMapReduceOperationSpec()
        .template AddOutput<W>(MaybeWithSchema<W>(to))
        .ReduceBy(hasDelim ? GetReduceByFields(reduceFields) : reduceFields);

    if (hasDelim) {
        spec.SortBy(GetSortByFields(reduceFields));
    }

    for (auto& input : from.Parts_) {
        spec.template AddInput<R>(input);
    }
    return spec;
}

template<class R, class W>
TReduceOperationSpec PrepareReduceSpec(
    const TOneOrMany<TRichYPath>& from,
    const TRichYPath& to,
    const TSortColumns& reduceFields)
{
    // SortBySep element separates ReduceBy fields from the rest of SortBy fields
    bool hasDelim = std::find(reduceFields.Parts_.begin(), reduceFields.Parts_.end(), SortBySep) != reduceFields.Parts_.end();
    TSortColumns reduceByFields = hasDelim ? GetReduceByFields(reduceFields) : reduceFields;

    auto spec = TReduceOperationSpec()
        .template AddOutput<W>(MaybeWithSchema<W>(to, reduceByFields))
        .ReduceBy(reduceByFields);

    if (hasDelim) {
        spec.SortBy(GetSortByFields(reduceFields));
    }

    for (auto& input : from.Parts_) {
        spec.template AddInput<R>(input);
    }
    return spec;
}

template <class Mapper>
bool RegisterMapperStatic() {
    REGISTER_MAPPER(Mapper);
    return true;
}

template <class Reducer>
bool RegisterReducerStatic() {
    REGISTER_REDUCER(Reducer);
    return true;
}

template<class Reducer, class TReduce, class TFinalize>
::TIntrusivePtr<IReducerBase> ChooseReducer(TReduce reducer, TFinalize finalizer, const TSortColumns& reduceColumns) {
    if (!reducer)
        return nullptr; // let main API generate an exception

    if (finalizer)
        return new Reducer(reducer, finalizer, reduceColumns);

    if (auto reducerObj = Reducer::SameWithoutFinalizer(reducer, reduceColumns))
        return reducerObj;

    ythrow yexception() << "finalizer can not be null";
}

// ==============================================
} // namespace NYT::NDetail
// ==============================================

// ==============================================
namespace NYT {
// ==============================================


/** Simple filter Mapper. Wraps [](const T& row){}.
 *  - lambda should return true to keep the row;
 */
template <class T>
class TCopyIfMapper : public NDetail::TLambdaOpBase<IMapper, T, T, bool (*)(const T&)> {
public:
    using F = bool (*)(const T&);
    using TBase = NDetail::TLambdaOpBase<IMapper, T, T, F>;

    TCopyIfMapper() { }
    TCopyIfMapper(F func) : TBase(func) {
        (void)Registrator; // make sure this static member is not optimized out
    }

    void Do(TTableReader<T>* reader, TTableWriter<T>* writer) override {
        for (; reader->IsValid(); reader->Next()) {
            const auto& curRow = reader->GetRow();
            if (TBase::Func(curRow)) {
                writer->AddRow(curRow);
            }
        }
    }

private:
    static bool Registrator;
};

template <class T>
bool TCopyIfMapper<T>::Registrator = NDetail::RegisterMapperStatic<TCopyIfMapper<T>>();


/** Mapper. Wraps [](const R& src, W& dst){}.
 *  - lambda should always fill the same columns in dst or clear dst before use;
 *  - return true to record dst or false to have this row dropped;
 */
template <class R, class W>
class TTransformMapper : public NDetail::TLambdaOpBase<IMapper, R, W, bool (*)(const R&, W&)> {
public:
    using F = bool (*)(const R&, W&);
    using TBase = NDetail::TLambdaOpBase<IMapper, R, W, F>;

    TTransformMapper() { }
    TTransformMapper(F func) : TBase(func) {
        (void)Registrator; // make sure this static member is not optimized out
    }

    void Do(TTableReader<R>* reader, TTableWriter<W>* writer) override {
        W writeBuf;
        for (; reader->IsValid(); reader->Next()) {
            const auto& curRow = reader->GetRow();
            if (TBase::Func(curRow, writeBuf)) {
                writer->AddRow(writeBuf);
            }
        }
    }

private:
    static bool Registrator;
};

template <class R, class W>
bool TTransformMapper<R, W>::Registrator = NDetail::RegisterMapperStatic<TTransformMapper<R, W>>();


/** Reducer between same type of records. Wraps [](const T& src, T& dst){}.
 *  - lambda should keep same columns in input and output;
 *  - lambda may not modify key columns (just don't touch them in dst);
 *  - lambda is not called for the first row of a reduce-key (it is copied to dst);
 *  - lambda is not called at all when there's just one row for a reduce-key;
 */
template <class T>
class TAdditiveReducer : public NDetail::TLambdaOpBase<IReducer, T, T, void (*)(const T&, T&)> {
public:
    using F = void (*)(const T&, T&);
    using TBase = NDetail::TLambdaOpBase<IReducer, T, T, F>;

    TAdditiveReducer() { }
    TAdditiveReducer(F func) : TBase(func) {
        (void)Registrator; // make sure this static member is not optimized out
    }

    void Do(TTableReader<T>* reader, TTableWriter<T>* writer) override {
        if (!reader->IsValid())
            return;
        T writeBuf = reader->GetRow();
        reader->Next();
        for (; reader->IsValid(); reader->Next()) {
            const auto& curRow = reader->GetRow();
            TBase::Func(curRow, writeBuf);
        }
        writer->AddRow(writeBuf);
    }

private:
    static bool Registrator;
};

template <class T>
bool TAdditiveReducer<T>::Registrator = NDetail::RegisterReducerStatic<TAdditiveReducer<T>>();


/** Reducer between two different types of records. Wraps [](const R& src, W& dst){}.
 *  - fields of dst that match reduceBy columns are filled by TLambdaReducer;
 *  - lambda may not modify these key columns (just don't touch them in dst);
 *  - other fields of dst are initialized (or not) by W's default constructor
 *    before lambda is called for the first row of a reduce-key;
 */
template <class R, class W>
class TLambdaReducer : public NDetail::TByColumnAwareLambdaOpBase<IReducer, R, W, void (*)(const R&, W&)> {
public:
    using F = void (*)(const R&, W&);
    using TBase = NDetail::TByColumnAwareLambdaOpBase<IReducer, R, W, F>;

    TLambdaReducer() { }
    TLambdaReducer(F func, const TSortColumns& reduceColumns)
        : TBase(func, reduceColumns)
    {
        (void)Registrator; // make sure this static member is not optimized out
    }

    void Do(TTableReader<R>* reader, TTableWriter<W>* writer) override {
        if (!reader->IsValid())
            return;
        W writeBuf;
        TBase::FieldCopier(reader->GetRow(), writeBuf);
        for (; reader->IsValid(); reader->Next()) {
            const auto& curRow = reader->GetRow();
            TBase::Func(curRow, writeBuf);
        }
        writer->AddRow(writeBuf);
    }

private:
    static bool Registrator;
};

template <class R, class W>
bool TLambdaReducer<R, W>::Registrator = NDetail::RegisterReducerStatic<TLambdaReducer<R, W>>();


/** Reducer between two different types of records, with intermediate structure.
 *  that allows to collect more data in process than it is written to output.
 *  Wraps [](const R& src, TBuf& buf){} and [](const TBuf& buf, W& dst){}
 *  - first lambda is called for every row;
 *  - second lambda is called once for each reduce-key after all rows have been processed,
 *    and generates actual record to be written;
 *  - fields of dst that match reduceBy columns are filled by TLambdaBufReducer;
 *  - lambda may not modify these key columns (just don't touch them in dst);
 *  - fields of buf are initialized (or not) by TBuf's default constructor
 *    before the first lambda is called for the first row of a reduce-key;
 *  - TBuf is not written anywhere, it can be any C++ type;
 */

template <class R, class TBuf, class W>
class TLambdaBufReducer : public NDetail::TByColumnAwareLambdaOpBase<IReducer, R, W,
        std::pair<void (*)(const R&, TBuf&), bool (*)(const TBuf&, W&)>> {
public:
    using TReduce = void (*)(const R&, TBuf&);
    using TFinalize = bool (*)(const TBuf&, W&);
    using TBase = NDetail::TByColumnAwareLambdaOpBase<IReducer, R, W, std::pair<TReduce, TFinalize>>;

    TLambdaBufReducer() { }
    TLambdaBufReducer(TReduce reducer, TFinalize finalizer, const TSortColumns& reduceColumns)
        : TBase({reducer, finalizer}, reduceColumns)
    {
        (void)Registrator; // make sure this static member is not optimized out
    }

    void Do(TTableReader<R>* reader, TTableWriter<W>* writer) override {
        if (!reader->IsValid())
            return;
        TBuf interim;
        W writeBuf;
        TBase::FieldCopier(reader->GetRow(), writeBuf);
        for (; reader->IsValid(); reader->Next()) {
            const auto& curRow = reader->GetRow();
            TBase::Func.first(curRow, interim);
        }

        if (TBase::Func.second(interim, writeBuf)) {
            writer->AddRow(writeBuf);
        }
    }

    static ::TIntrusivePtr<IReducerBase> SameWithoutFinalizer(TReduce reducer, const TSortColumns& reduceColumns) {
        if constexpr(std::is_same<TBuf, W>::value) {
            return new TLambdaReducer<R, W>(reducer, reduceColumns);
        }
        return nullptr;
    }

private:
    static bool Registrator;
};

template <class R, class TBuf, class W>
bool TLambdaBufReducer<R, TBuf, W>::Registrator = NDetail::RegisterReducerStatic<TLambdaBufReducer<R, TBuf, W>>();

/** Reducer between two different types of records, with intermediate structure
 *  of the same type as input record, that allows to collect more data in process
 *  than it is written to output.
 *  Wraps [](const R& src, R& buf){} and [](const R& buf, W& dst){}
 *  - first lambda is not called for the first row of a reduce-key (it is copied to buffer);
 *  - first lambda is not called at all when there's just one row for a reduce-key;
 *  - second lambda is called once for each reduce-key after all rows have been processed,
 *    and generates actual record to be written;
 *  - fields of dst that match reduceBy columns are filled by TAdditiveLambdaBufReducer;
 *  - lambda may not modify these key columns (just don't touch them in dst);
 */

template <class R, class W>
class TAdditiveLambdaBufReducer : public NDetail::TByColumnAwareLambdaOpBase<IReducer, R, W,
        std::pair<void (*)(const R&, R&), bool (*)(const R&, W&)>> {
public:
    using TReduce = void (*)(const R&, R&);
    using TFinalize = bool (*)(const R&, W&);
    using TBase = NDetail::TByColumnAwareLambdaOpBase<IReducer, R, W, std::pair<TReduce, TFinalize>>;

    TAdditiveLambdaBufReducer() { }
    TAdditiveLambdaBufReducer(TReduce reducer, TFinalize finalizer, const TSortColumns& reduceColumns)
        : TBase({reducer, finalizer}, reduceColumns)
    {
        (void)Registrator; // make sure this static member is not optimized out
    }

    void Do(TTableReader<R>* reader, TTableWriter<W>* writer) override {
        if (!reader->IsValid())
            return;
        R interim = reader->GetRow();
        reader->Next();
        for (; reader->IsValid(); reader->Next()) {
            const auto& curRow = reader->GetRow();
            TBase::Func.first(curRow, interim);
        }

        W writeBuf;
        TBase::FieldCopier(interim, writeBuf);
        if (TBase::Func.second(interim, writeBuf)) {
            writer->AddRow(writeBuf);
        }
    }

    static ::TIntrusivePtr<IReducerBase> SameWithoutFinalizer(TReduce reducer, const TSortColumns&) {
        if constexpr(std::is_same<R, W>::value) {
            return new TAdditiveReducer<W>(reducer);
        }
        return nullptr;
    }

private:
    static bool Registrator;
};

template <class R, class W>
bool TAdditiveLambdaBufReducer<R, W>::Registrator =
    NDetail::RegisterReducerStatic<TAdditiveLambdaBufReducer<R, W>>();

// ==============================================
} // namespace NYT
// ==============================================
