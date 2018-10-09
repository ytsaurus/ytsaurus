#pragma once

#include <mapreduce/yt/interface/client.h>
#include <util/ysaveload.h>

// ==============================================
namespace NYT::NDetail {
// ==============================================

template <template<class Rd, class Wr> class Op, class R, class W, class F>
class TLambdaOpBase : public Op<TTableReader<R>, TTableWriter<W>> {
public:
    TLambdaOpBase() { }
    TLambdaOpBase(F func) : Func(func) { }

    void Save(IOutputStream& stream) const override {
        // Note that this does not work with any lambda,
        // actually Func is a function pointer for now.
        SavePodType(&stream, Func);
    }
    void Load(IInputStream& stream) override {
        LoadPodType(&stream, Func);
    }

protected:
    F Func;
};

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

// ==============================================
} // namespace NYT
// ==============================================
