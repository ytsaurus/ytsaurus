#pragma once

#ifndef IO_INL_H_
#error "Direct inclusion of this file is not allowed, use io.h"
#endif
#undef IO_INL_H_

#include "finish_or_die.h"

#include <util/generic/typetraits.h>
#include <util/generic/yexception.h>
#include <util/stream/length.h>

#include <util/system/mutex.h>
#include <util/system/spinlock.h>

#include <library/cpp/yson/node/node_builder.h>

#include <mapreduce/yt/interface/serialize.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template<class T>
struct TIsYdlOneOf
    : std::false_type
{ };

template<class... TYdlRowTypes>
struct TIsYdlOneOf<TYdlOneOf<TYdlRowTypes...>>
    : std::true_type
{ };

template<class T>
struct TIsProtoOneOf
    : std::false_type
{ };

template <class ...TProtoRowTypes>
struct TIsProtoOneOf<TProtoOneOf<TProtoRowTypes...>>
    : std::true_type
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TRowTraits;

template <>
struct TRowTraits<TNode>
{
    using TRowType = TNode;
    using IReaderImpl = INodeReaderImpl;
    using IWriterImpl = INodeWriterImpl;
};

template <>
struct TRowTraits<TYaMRRow>
{
    using TRowType = TYaMRRow;
    using IReaderImpl = IYaMRReaderImpl;
    using IWriterImpl = IYaMRWriterImpl;
};

template <>
struct TRowTraits<Message>
{
    using TRowType = Message;
    using IReaderImpl = IProtoReaderImpl;
    using IWriterImpl = IProtoWriterImpl;
};

template <class T>
struct TRowTraits<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
{
    using TRowType = T;
    using IReaderImpl = IProtoReaderImpl;
    using IWriterImpl = IProtoWriterImpl;
};

template <>
struct TRowTraits<NDetail::TYdlGenericRowType>
{
    using TRowType = NDetail::TYdlGenericRowType;
    using IReaderImpl = IYdlReaderImpl;
    using IWriterImpl = IYdlWriterImpl;
};

template <class T>
struct TRowTraits<T, std::enable_if_t<NYdl::TIsYdlGenerated<T>::value>>
{
    using TRowType = T;
    using IReaderImpl = IYdlReaderImpl;
    using IWriterImpl = IYdlWriterImpl;
};

template <class... TYdlRowTypes>
struct TRowTraits<TYdlOneOf<TYdlRowTypes...>>
{
    using TRowType = TYdlOneOf<TYdlRowTypes...>;
    using IReaderImpl = IYdlReaderImpl;
    using IWriterImpl = IYdlWriterImpl;
};

template <class... TProtoRowTypes>
struct TRowTraits<TProtoOneOf<TProtoRowTypes...>>
{
    using TRowType = TProtoOneOf<TProtoRowTypes...>;
    using IReaderImpl = IProtoReaderImpl;
    using IWriterImpl = IProtoWriterImpl;
};

////////////////////////////////////////////////////////////////////////////////

struct IReaderImplBase
    : public TThrRefBase
{
    virtual bool IsValid() const = 0;
    virtual void Next() = 0;
    virtual ui32 GetTableIndex() const = 0;
    virtual ui32 GetRangeIndex() const = 0;
    virtual ui64 GetRowIndex() const = 0;
    virtual void NextKey() = 0;

    // Not pure virtual because of clients that has already implemented this interface.
    virtual TMaybe<size_t> GetReadByteCount() const;
    virtual i64 GetTabletIndex() const;
    virtual bool IsEndOfStream() const;
    virtual bool IsRawReaderExhausted() const;
};

struct INodeReaderImpl
    : public IReaderImplBase
{
    virtual const TNode& GetRow() const = 0;
    virtual void MoveRow(TNode* row) = 0;
};

struct IYaMRReaderImpl
    : public IReaderImplBase
{
    virtual const TYaMRRow& GetRow() const = 0;
    virtual void MoveRow(TYaMRRow* row)
    {
        *row = GetRow();
    }
};

struct IYdlReaderImpl
    : public IReaderImplBase
{
    virtual const TNode& GetRow() const = 0;
    virtual void VerifyRowType(ui64 rowTypeHash) const = 0;
};

struct IProtoReaderImpl
    : public IReaderImplBase
{
    virtual void ReadRow(Message* row) = 0;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

// We don't include <mapreduce/yt/interface/logging/log.h> in this file
// to avoid macro name clashes (specifically LOG_DEBUG)
void LogTableReaderStatistics(ui64 rowCount, TMaybe<size_t> byteCount);

template <class T>
class TTableReaderBase
    : public TThrRefBase
{
public:
    using TRowType = T;
    using IReaderImpl = typename TRowTraits<T>::IReaderImpl;

    explicit TTableReaderBase(::TIntrusivePtr<IReaderImpl> reader)
        : Reader_(reader)
    { }

    ~TTableReaderBase()
    {
        NDetail::LogTableReaderStatistics(ReadRowCount_, Reader_->GetReadByteCount());
    }

    bool IsValid() const
    {
        return Reader_->IsValid();
    }

    void Next()
    {
        Reader_->Next();
        ++ReadRowCount_;
        RowState_ = ERowState::None;
    }

    bool IsEndOfStream()
    {
        return Reader_->IsEndOfStream();
    }

    bool IsRawReaderExhausted()
    {
        return Reader_->IsRawReaderExhausted();
    }

    ui32 GetTableIndex() const
    {
        return Reader_->GetTableIndex();
    }

    ui32 GetRangeIndex() const
    {
        return Reader_->GetRangeIndex();
    }

    ui64 GetRowIndex() const
    {
        return Reader_->GetRowIndex();
    }

    i64 GetTabletIndex() const
    {
        return Reader_->GetTabletIndex();
    }

protected:
    template <typename TCacher, typename TCacheGetter>
    const auto& DoGetRowCached(TCacher cacher, TCacheGetter cacheGetter) const
    {
        switch (RowState_) {
            case ERowState::None:
                cacher();
                RowState_ = ERowState::Cached;
                break;
            case ERowState::Cached:
                break;
            case ERowState::MovedOut:
                ythrow yexception() << "Row is already moved";
        }
        return *cacheGetter();
    }

    template <typename U, typename TMover, typename TCacheMover>
    void DoMoveRowCached(U* result, TMover mover, TCacheMover cacheMover)
    {
        Y_VERIFY(result);
        switch (RowState_) {
            case ERowState::None:
                mover(result);
                break;
            case ERowState::Cached:
                cacheMover(result);
                break;
            case ERowState::MovedOut:
                ythrow yexception() << "Row is already moved";
        }
        RowState_ = ERowState::MovedOut;
    }

private:
    enum class ERowState
    {
        None,
        Cached,
        MovedOut,
    };

protected:
    ::TIntrusivePtr<IReaderImpl> Reader_;

private:
    ui64 ReadRowCount_ = 0;
    mutable ERowState RowState_ = ERowState::None;
};

template <class T>
class TSimpleTableReader
    : public TTableReaderBase<T>
{
public:
    using TBase = TTableReaderBase<T>;
    using typename TBase::TRowType;

    using TBase::TBase;

    const TRowType& GetRow() const
    {
        // Caching is implemented in underlying reader.
        return TBase::DoGetRowCached(
            /* cacher */ [&] {},
            /* cacheGetter */ [&] {
                return &Reader_->GetRow();
            });
    }

    void MoveRow(TRowType* result)
    {
        // Caching is implemented in underlying reader.
        TBase::DoMoveRowCached(
            result,
            /* mover */ [&] (TRowType* result) {
                Reader_->MoveRow(result);
            },
            /* cacheMover */ [&] (TRowType* result) {
                Reader_->MoveRow(result);
            });
    }

    TRowType MoveRow()
    {
        TRowType result;
        MoveRow(&result);
        return result;
    }

private:
    using TBase::Reader_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <>
class TTableReader<TNode>
    : public NDetail::TSimpleTableReader<TNode>
{
    using TSimpleTableReader<TNode>::TSimpleTableReader;
};

template <>
class TTableReader<TYaMRRow>
    : public NDetail::TSimpleTableReader<TYaMRRow>
{
    using TSimpleTableReader<TYaMRRow>::TSimpleTableReader;
};

template<class... TYdlRowTypes>
class TTableReader<TYdlOneOf<TYdlRowTypes...>>
    : public NDetail::TTableReaderBase<TYdlOneOf<TYdlRowTypes...>>
{
public:
    using TBase = NDetail::TTableReaderBase<TYdlOneOf<TYdlRowTypes...>>;

    using TBase::TBase;

    template <class U>
    const U& GetRow() const
    {
        AssertIsOneOf<U>();
        Reader_->VerifyRowType(NYdl::TYdlTraits<U>::ReflectRaw()->GetHash());
        return TBase::DoGetRowCached(
            /* cacher */ [&] {
                std::get<U>(CachedRows_) = U::DeserializeFromYson(Reader_->GetRow());
            },
            /* cacheGetter */ [&] {
                return &std::get<U>(CachedRows_);
            });
    }

    template <class U>
    void MoveRow(U* result)
    {
        AssertIsOneOf<U>();
        Reader_->VerifyRowType(NYdl::TYdlTraits<U>::ReflectRaw()->GetHash());
        return TBase::DoMoveRowCached(
            result,
            /* mover */ [&] (U* result) {
                *result = U::DeserializeFromYson(Reader_->GetRow());
            },
            /* cacheMover */ [&] (U* result) {
                *result = std::move(std::get<U>(CachedRows_));
            });
    }

    template <class U>
    U MoveRow()
    {
        U result;
        MoveRow(&result);
        return result;
    }

private:
    using TBase::Reader_;
    // std::variant could also be used here, but std::tuple leads to better performance
    // because of deallocations that std::variant has to do
    mutable std::tuple<TYdlRowTypes...> CachedRows_;

    template <class U>
    static constexpr void AssertIsOneOf()
    {
        static_assert(
            (std::is_same<U, TYdlRowTypes>::value || ...),
            "Template parameter must be one of TYdlOneOf types");
    }
};

template <class T>
class TTableReader<T, std::enable_if_t<NYdl::TIsYdlGenerated<T>::value>>
   : public TTableReader<TYdlOneOf<T>>
{
public:
    using TBase = TTableReader<TYdlOneOf<T>>;
    using TRowType = T;

    using TBase::TBase;

    const TRowType& GetRow() const
    {
        return TBase::template GetRow<TRowType>();
    }

    void MoveRow(TRowType* result)
    {
        TBase::template MoveRow<TRowType>(result);
    }

    T MoveRow()
    {
        return TBase::template MoveRow<TRowType>();
    }
};

template <>
class TTableReader<Message>
    : public NDetail::TTableReaderBase<Message>
{
public:
    using TBase = NDetail::TTableReaderBase<Message>;

    using TBase::TBase;

    template <class U>
    const U& GetRow() const
    {
        static_assert(TIsBaseOf<Message, U>::Value);

        return TBase::DoGetRowCached(
            /* cacher */ [&] {
                CachedRow_.Reset(new U);
                Reader_->ReadRow(CachedRow_.Get());
            },
            /* cacheGetter */ [&] {
                auto result = dynamic_cast<const U*>(CachedRow_.Get());
                Y_VERIFY(result);
                return result;
            });
    }

    template <class U>
    void MoveRow(U* result)
    {
        static_assert(TIsBaseOf<Message, U>::Value);

        TBase::DoMoveRowCached(
            result,
            /* mover */ [&] (U* result) {
                Reader_->ReadRow(result);
            },
            /* cacheMover */ [&] (U* result) {
                auto cast = dynamic_cast<U*>(CachedRow_.Get());
                Y_VERIFY(cast);
                result->Swap(cast);
            });
    }

    template <class U>
    U MoveRow()
    {
        static_assert(TIsBaseOf<Message, U>::Value);

        U result;
        MoveRow(&result);
        return result;
    }

    ::TIntrusivePtr<IProtoReaderImpl> GetReaderImpl() const
    {
        return Reader_;
    }

private:
    using TBase::Reader_;
    mutable THolder<Message> CachedRow_;
};


template<class... TProtoRowTypes>
class TTableReader<TProtoOneOf<TProtoRowTypes...>>
    : public NDetail::TTableReaderBase<TProtoOneOf<TProtoRowTypes...>>
{
public:
    using TBase = NDetail::TTableReaderBase<TProtoOneOf<TProtoRowTypes...>>;

    using TBase::TBase;

    template <class U>
    const U& GetRow() const
    {
        AssertIsOneOf<U>();
        return TBase::DoGetRowCached(
            /* cacher */ [&] {
                Reader_->ReadRow(&std::get<U>(CachedRows_));
                CachedIndex_ = NDetail::TIndexInTuple<U, decltype(CachedRows_)>::Value;
            },
            /* cacheGetter */ [&] {
                return &std::get<U>(CachedRows_);
            });
    }

    template <class U>
    void MoveRow(U* result)
    {
        AssertIsOneOf<U>();
        return TBase::DoMoveRowCached(
            result,
            /* mover */ [&] (U* result) {
                Reader_->ReadRow(result);
            },
            /* cacheMover */ [&] (U* result) {
                Y_VERIFY((NDetail::TIndexInTuple<U, decltype(CachedRows_)>::Value) == CachedIndex_);
                *result = std::move(std::get<U>(CachedRows_));
            });
    }

    template <class U>
    U MoveRow()
    {
        U result;
        MoveRow(&result);
        return result;
    }

    ::TIntrusivePtr<IProtoReaderImpl> GetReaderImpl() const
    {
        return Reader_;
    }

private:
    using TBase::Reader_;
    // std::variant could also be used here, but std::tuple leads to better performance
    // because of deallocations that std::variant has to do
    mutable std::tuple<TProtoRowTypes...> CachedRows_;
    mutable int CachedIndex_;

    template <class U>
    static constexpr void AssertIsOneOf()
    {
        static_assert(
            (std::is_same<U, TProtoRowTypes>::value || ...),
            "Template parameter must be one of TProtoOneOf template parameter");
    }
};

template <class T>
class TTableReader<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
    : public TTableReader<TProtoOneOf<T>>
{
public:
    using TRowType = T;
    using TBase = TTableReader<TProtoOneOf<T>>;

    using TBase::TBase;

    const T& GetRow() const
    {
        return TBase::template GetRow<T>();
    }

    void MoveRow(T* result)
    {
        TBase::template MoveRow<T>(result);
    }

    T MoveRow()
    {
        return TBase::template MoveRow<T>();
    }
};

template <>
inline TTableReaderPtr<TNode> IIOClient::CreateTableReader<TNode>(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    return new TTableReader<TNode>(CreateNodeReader(path, options));
}

template <>
inline TTableReaderPtr<TYaMRRow> IIOClient::CreateTableReader<TYaMRRow>(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    return new TTableReader<TYaMRRow>(CreateYaMRReader(path, options));
}

template <class T, class = std::enable_if_t<TIsBaseOf<Message, T>::Value>>
struct TReaderCreator
{
    static TTableReaderPtr<T> Create(::TIntrusivePtr<IProtoReaderImpl> reader)
    {
        return new TTableReader<T>(reader);
    }
};

template <class T>
inline TTableReaderPtr<T> IIOClient::CreateTableReader(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    if constexpr (TIsBaseOf<Message, T>::Value) {
        TAutoPtr<T> prototype(new T);
        return new TTableReader<T>(CreateProtoReader(path, options, prototype.Get()));
    } else if constexpr (NYdl::TIsYdlGenerated<T>::value) {
        return new TTableReader<T>(CreateYdlReader(path, options, NYdl::TYdlTraits<T>::Reflect()));
    } else {
        static_assert(TDependentFalse<T>, "Unsupported type for table reader");
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateTableReader(
    IInputStream* stream,
    const TTableReaderOptions& options)
{
    return TReaderCreator<T>::Create(NDetail::CreateProtoReader(stream, options, T::descriptor()));
}

template <class... Ts>
TTableReaderPtr<typename NDetail::TProtoOneOfUnique<Ts...>::TType> CreateProtoMultiTableReader(
    IInputStream* stream,
    const TTableReaderOptions& options)
{
    return new TTableReader<typename NDetail::TProtoOneOfUnique<Ts...>::TType>(
        NDetail::CreateProtoReader(stream, options, {Ts::descriptor()...}));
}

template <class T>
TTableReaderPtr<T> CreateProtoMultiTableReader(
    IInputStream* stream,
    int tableCount,
    const TTableReaderOptions& options)
{
    static_assert(TIsBaseOf<::google::protobuf::Message, T>::Value);
    TVector<const ::google::protobuf::Descriptor*> descriptors(tableCount, T::descriptor());
    return new TTableReader<T>(NDetail::CreateProtoReader(stream, options, std::move(descriptors)));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TTableRangesReader<T>
    : public TThrRefBase
{
public:
    using TRowType = T;

private:
    using TReaderImpl = typename TRowTraits<TRowType>::IReaderImpl;

public:
    TTableRangesReader(::TIntrusivePtr<TReaderImpl> readerImpl)
        : ReaderImpl_(readerImpl)
        , Reader_(MakeIntrusive<TTableReader<TRowType>>(readerImpl))
        , IsValid_(Reader_->IsValid())
    { }

    TTableReader<T>& GetRange()
    {
        return *Reader_;
    }

    bool IsValid() const
    {
        return IsValid_;
    }

    void Next()
    {
        ReaderImpl_->NextKey();
        if ((IsValid_ = Reader_->IsValid())) {
            Reader_->Next();
        }
    }

private:
    ::TIntrusivePtr<TReaderImpl> ReaderImpl_;
    ::TIntrusivePtr<TTableReader<TRowType>> Reader_;
    bool IsValid_;
};

////////////////////////////////////////////////////////////////////////////////

struct IWriterImplBase
    : public TThrRefBase
{
    virtual size_t GetTableCount() const = 0;
    virtual void FinishTable(size_t tableIndex) = 0;
    virtual void Abort()
    { }
};

struct INodeWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TNode& row, size_t tableIndex) = 0;
    virtual void AddRow(TNode&& row, size_t tableIndex) = 0;
};

struct IYaMRWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TYaMRRow& row, size_t tableIndex) = 0;
    virtual void AddRow(TYaMRRow&& row, size_t tableIndex) = 0;
};

struct IYdlWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TNode& row, size_t tableIndex) = 0;
    virtual void AddRow(TNode&& row, size_t tableIndex) = 0;
    virtual void VerifyRowType(ui64 rowTypeHash, size_t tableIndex) const = 0;
};

struct IProtoWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const Message& row, size_t tableIndex) = 0;
    virtual void AddRow(Message&& row, size_t tableIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TTableWriterBase
    : public TThrRefBase
{
public:
    using TRowType = T;
    using IWriterImpl = typename TRowTraits<T>::IWriterImpl;

    explicit TTableWriterBase(::TIntrusivePtr<IWriterImpl> writer)
        : Writer_(writer)
        , Locks_(MakeAtomicShared<TVector<TAdaptiveLock>>(writer->GetTableCount()))
    { }

    ~TTableWriterBase() override
    {
        if (Locks_.RefCount() == 1) {
            NDetail::FinishOrDie(this, "TTableWriterBase");
        }
    }

    void Abort()
    {
        Writer_->Abort();
    }

    void AddRow(const T& row, size_t tableIndex = 0)
    {
        DoAddRow<T>(row, tableIndex);
    }
    void AddRow(T&& row, size_t tableIndex = 0)
    {
        DoAddRow<T>(std::move(row), tableIndex);
    }

    void Finish()
    {
        for (size_t i = 0; i < Locks_->size(); ++i) {
            auto guard = Guard((*Locks_)[i]);
            Writer_->FinishTable(i);
        }
    }

protected:
    template <class U>
    void DoAddRow(const U& row, size_t tableIndex = 0)
    {
        if (tableIndex >= Locks_->size()) {
            ythrow TIOException() <<
                "Table index " << tableIndex <<
                " is out of range [0, " << Locks_->size() << ")";
        }

        auto guard = Guard((*Locks_)[tableIndex]);
        Writer_->AddRow(row, tableIndex);
    }
    template <class U>
    void DoAddRow(U&& row, size_t tableIndex = 0)
    {
        if (tableIndex >= Locks_->size()) {
            ythrow TIOException() <<
                "Table index " << tableIndex <<
                " is out of range [0, " << Locks_->size() << ")";
        }

        auto guard = Guard((*Locks_)[tableIndex]);
        Writer_->AddRow(std::move(row), tableIndex);
    }

    ::TIntrusivePtr<IWriterImpl> GetWriterImpl()
    {
        return Writer_;
    }

private:
    ::TIntrusivePtr<IWriterImpl> Writer_;
    TAtomicSharedPtr<TVector<TAdaptiveLock>> Locks_;
};

template <>
class TTableWriter<TNode>
    : public TTableWriterBase<TNode>
{
public:
    using TBase = TTableWriterBase<TNode>;

    explicit TTableWriter(::TIntrusivePtr<IWriterImpl> writer)
        : TBase(writer)
    { }
};

template <>
class TTableWriter<TYaMRRow>
    : public TTableWriterBase<TYaMRRow>
{
public:
    using TBase = TTableWriterBase<TYaMRRow>;

    explicit TTableWriter(::TIntrusivePtr<IWriterImpl> writer)
        : TBase(writer)
    { }
};

template <>
class TTableWriter<NDetail::TYdlGenericRowType>
    : public TTableWriterBase<NDetail::TYdlGenericRowType>
{
public:
    using TBase = TTableWriterBase<NDetail::TYdlGenericRowType>;

    explicit TTableWriter(::TIntrusivePtr<IWriterImpl> writer)
        : TBase(writer)
    { }

    template<class U, std::enable_if_t<NYdl::TIsYdlGenerated<U>::value>* = nullptr>
    void AddRow(const U& row, size_t tableIndex = 0)
    {
        GetWriterImpl()->VerifyRowType(NYdl::TYdlTraits<U>::ReflectRaw()->GetHash(), tableIndex);
        TNode node;
        TNodeBuilder builder(&node);
        row.SerializeAsYson(builder);
        TBase::DoAddRow(node, tableIndex);
    }
};

template <class T>
class TTableWriter<T, std::enable_if_t<NYdl::TIsYdlGenerated<T>::value>>
   : public TTableWriter<NDetail::TYdlGenericRowType>
{
public:
    using TBase = TTableWriter<NDetail::TYdlGenericRowType>;
    using TRowType = T;

    explicit TTableWriter(::TIntrusivePtr<IWriterImpl> writer)
        : TBase(std::move(writer))
    { }

    void AddRow(const T& row, size_t tableIndex = 0)
    {
        TBase::template AddRow<T>(row, tableIndex);
    }
};

template <>
class TTableWriter<Message>
    : public TTableWriterBase<Message>
{
public:
    using TBase = TTableWriterBase<Message>;

    explicit TTableWriter(::TIntrusivePtr<IWriterImpl> writer)
        : TBase(writer)
    { }

    template <class U, std::enable_if_t<std::is_base_of<Message, U>::value>* = nullptr>
    void AddRow(const U& row, size_t tableIndex = 0)
    {
        TBase::AddRow(row, tableIndex);
    }
};

template <class T>
class TTableWriter<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
    : public TTableWriter<Message>
{
public:
    using TRowType = T;
    using TBase = TTableWriter<Message>;

    explicit TTableWriter(::TIntrusivePtr<IWriterImpl> writer)
        : TBase(writer)
    { }

    void AddRow(const T& row, size_t tableIndex = 0)
    {
        TBase::AddRow<T>(row, tableIndex);
    }
};

template <>
inline TTableWriterPtr<TNode> IIOClient::CreateTableWriter<TNode>(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    return new TTableWriter<TNode>(CreateNodeWriter(path, options));
}

template <>
inline TTableWriterPtr<TYaMRRow> IIOClient::CreateTableWriter<TYaMRRow>(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    return new TTableWriter<TYaMRRow>(CreateYaMRWriter(path, options));
}

template <class T>
inline TTableWriterPtr<T> IIOClient::CreateTableWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    if constexpr (TIsBaseOf<Message, T>::Value) {
        TAutoPtr<T> prototype(new T);
        return new TTableWriter<T>(CreateProtoWriter(path, options, prototype.Get()));
    } else if constexpr (NYdl::TIsYdlGenerated<T>::value) {
        return new TTableWriter<T>(CreateYdlWriter(path, options, NYdl::TYdlTraits<T>::Reflect()));
    } else {
        static_assert(TDependentFalse<T>, "Unsupported type for table writer");
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateConcreteProtobufReader(TTableReader<Message>* reader)
{
    static_assert(std::is_base_of_v<Message, T>, "T must be a protobuf type (either Message or its descendant)");
    Y_ENSURE(reader, "reader must be non-null");
    return ::MakeIntrusive<TTableReader<T>>(reader->GetReaderImpl());
}

template <typename T>
TTableReaderPtr<T> CreateConcreteProtobufReader(const TTableReaderPtr<Message>& reader)
{
    Y_ENSURE(reader, "reader must be non-null");
    return CreateConcreteProtobufReader<T>(reader.Get());
}

template <typename T>
TTableReaderPtr<Message> CreateGenericProtobufReader(TTableReader<T>* reader)
{
    static_assert(std::is_base_of_v<Message, T>, "T must be a protobuf type (either Message or its descendant)");
    Y_ENSURE(reader, "reader must be non-null");
    return ::MakeIntrusive<TTableReader<Message>>(reader->GetReaderImpl());
}

template <typename T>
TTableReaderPtr<Message> CreateGenericProtobufReader(const TTableReaderPtr<T>& reader)
{
    Y_ENSURE(reader, "reader must be non-null");
    return CreateGenericProtobufReader(reader.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
