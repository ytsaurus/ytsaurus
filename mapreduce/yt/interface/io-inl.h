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

namespace NYT {

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

struct IProtoReaderImpl
    : public IReaderImplBase
{
    virtual void ReadRow(Message* row) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// We don't include <mapreduce/yt/interface/logging/log.h> in this file
// to avoid macro name clashes (specifically LOG_DEBUG)
namespace NDetail {
    void LogTableReaderStatistics(ui64 rowCount, TMaybe<size_t> byteCount);
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TTableReaderBase
    : public TThrRefBase
{
public:
    using TRowType = T;
    using IReaderImpl = typename TRowTraits<T>::IReaderImpl;

    TTableReaderBase()
    { }

    ~TTableReaderBase()
    {
        NDetail::LogTableReaderStatistics(ReadRowCount_, Reader_->GetReadByteCount());
    }

    explicit TTableReaderBase(::TIntrusivePtr<IReaderImpl> reader)
        : Reader_(reader)
    { }

    const T& GetRow() const
    {
        return Reader_->GetRow();
    }

    void MoveRow(T* row)
    {
        Y_VERIFY(row);
        return Reader_->MoveRow(row);
    }

    T MoveRow()
    {
        T result;
        Reader_->MoveRow(&result);
        return result;
    }

    bool IsValid() const
    {
        return Reader_->IsValid();
    }

    void Next()
    {
        Reader_->Next();
        ++ReadRowCount_;
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

    void Abort()
    {
        Reader_->Abort();
    }

private:
    ::TIntrusivePtr<IReaderImpl> Reader_;
    ui64 ReadRowCount_ = 0;
};

template <>
class TTableReader<TNode>
    : public TTableReaderBase<TNode>
{
public:
    using TBase = TTableReaderBase<TNode>;

    explicit TTableReader(::TIntrusivePtr<IReaderImpl> reader)
        : TBase(reader)
    { }
};

template <>
class TTableReader<TYaMRRow>
    : public TTableReaderBase<TYaMRRow>
{
public:
    using TBase = TTableReaderBase<TYaMRRow>;

    explicit TTableReader(::TIntrusivePtr<IReaderImpl> reader)
        : TBase(reader)
    { }
};

template <>
class TTableReader<Message>
    : public TThrRefBase
{
public:
    using TRowType = Message;

    explicit TTableReader(::TIntrusivePtr<IProtoReaderImpl> reader)
        : Reader_(reader)
    { }

    template <class U, std::enable_if_t<TIsBaseOf<Message, U>::Value>* = nullptr>
    const U& GetRow() const
    {
        if (!CachedRow_) {
            THolder<Message> row(new U);
            ReadRow(row.Get());
            CachedRow_.Swap(row);
        }
        return dynamic_cast<const U&>(*CachedRow_);
    }

    template <class U, std::enable_if_t<TIsBaseOf<Message, U>::Value>* = nullptr>
    void MoveRow(U* result)
    {
        Y_VERIFY(result != nullptr);
        U row;
        if (CachedRow_) {
            row.Swap(&dynamic_cast<U&>(*CachedRow_));
            CachedRow_.Reset();
        } else {
            ReadRow(&row);
        }
        result->Swap(&row);
    }

    template <class U, std::enable_if_t<TIsBaseOf<Message, U>::Value>* = nullptr>
    U MoveRow()
    {
        U result;
        MoveRow(&result);
        return result;
    }

    bool IsValid() const
    {
        return Reader_->IsValid();
    }

    void Next()
    {
        Reader_->Next();
        CachedRow_.Reset(nullptr);
        RowDone_ = false;
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

    void ReadRow(Message* row) const
    {
        //Not all the IProtoReaderImpl implementations support multiple ReadRow calls
        //TODO: fix LSP violation
        Y_ENSURE(!RowDone_, "Row is already moved");
        Reader_->ReadRow(row);
        RowDone_ = true;
    }

    ::TIntrusivePtr<IProtoReaderImpl> GetReaderImpl() const
    {
        return Reader_;
    }

private:
    ::TIntrusivePtr<IProtoReaderImpl> Reader_;
    mutable THolder<Message> CachedRow_;
    mutable bool RowDone_ = false;
};

template <class T>
class TTableReader<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
    : public TThrRefBase
{
public:
    using TRowType = T;

    explicit TTableReader(::TIntrusivePtr<IProtoReaderImpl> reader)
        : Reader_(std::move(reader))
    { }

    const TRowType& GetRow() const
    {
        switch (RowState_) {
            case None:
                Reader_->ReadRow(&CachedRow_);
                RowState_ = Cached;
                return CachedRow_;
            case Cached:
                return CachedRow_;
            case MovedOut:
                ythrow yexception() << "Row is already moved";
        }
        Y_FAIL();
    }

    void MoveRow(TRowType* result)
    {
        Y_VERIFY(result != nullptr);
        switch (RowState_) {
            case None:
                Reader_->ReadRow(result);
                RowState_ = MovedOut;
                return;
            case Cached:
                result->Swap(&CachedRow_);
                RowState_ = MovedOut;
                return;
            case MovedOut:
                ythrow yexception() << "Row is already moved";
        }
        Y_FAIL();
    }

    TRowType MoveRow()
    {
        TRowType result;
        MoveRow(&result);
        return result;
    }

    bool IsValid() const
    {
        return Reader_->IsValid();
    }

    void Next()
    {
        Reader_->Next();
        RowState_ = None;
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

    ::TIntrusivePtr<IProtoReaderImpl> GetReaderImpl() const
    {
        return Reader_;
    }

private:
    ::TIntrusivePtr<IProtoReaderImpl> Reader_;
    mutable TRowType CachedRow_;

    enum ERowState {
        None,
        Cached,
        MovedOut,
    };
    mutable ERowState RowState_ = None;
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
    TAutoPtr<T> prototype(new T);
    return TReaderCreator<T>::Create(CreateProtoReader(path, options, prototype.Get()));
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateTableReader(
    IInputStream* stream, const TTableReaderOptions& options)
{
    return TReaderCreator<T>::Create(NDetail::CreateProtoReader(stream, options, T::descriptor()));
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
    virtual size_t GetStreamCount() const = 0;
    virtual IOutputStream* GetStream(size_t tableIndex) const = 0;
    virtual void Abort()
    { }
};

struct INodeWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TNode& row, size_t tableIndex) = 0;
};

struct IYaMRWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TYaMRRow& row, size_t tableIndex) = 0;
};

struct IProtoWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const Message& row, size_t tableIndex) = 0;
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
        , Locks_(MakeAtomicShared<TVector<TMutex>>(writer->GetStreamCount()))
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
        if (tableIndex >= Locks_->size()) {
            ythrow TIOException() <<
                "Table index " << tableIndex <<
                " is out of range [0, " << Locks_->size() << ")";
        }

        auto guard = Guard((*Locks_)[tableIndex]);
        Writer_->AddRow(row, tableIndex);
    }

    void Finish()
    {
        for (size_t i = 0; i < Writer_->GetStreamCount(); ++i) {
            auto guard = Guard((*Locks_)[i]);
            Writer_->GetStream(i)->Finish();
        }
    }

private:
    ::TIntrusivePtr<IWriterImpl> Writer_;
    TAtomicSharedPtr<TVector<TMutex>> Locks_;
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
class TTableWriter<Message>
    : public TThrRefBase
{
public:
    using TRowType = Message;

    explicit TTableWriter(::TIntrusivePtr<IProtoWriterImpl> writer)
        : Writer_(writer)
        , Locks_(writer->GetStreamCount())
    { }

    ~TTableWriter() override
    {
        try {
            Finish();
        } catch (...) {
            // no guarantees
        }
    }

    template <class U, std::enable_if_t<std::is_base_of<Message, U>::value>* = nullptr>
    void AddRow(const U& row, size_t tableIndex = 0)
    {
        auto guard = Guard(Locks_[tableIndex]);
        Writer_->AddRow(row, tableIndex);
    }

    void Finish()
    {
        for (size_t i = 0; i < Writer_->GetStreamCount(); ++i) {
            auto guard = Guard(Locks_[i]);
            Writer_->GetStream(i)->Finish();
        }
    }

private:
    ::TIntrusivePtr<IProtoWriterImpl> Writer_;
    TVector<TMutex> Locks_;
};

template <class T>
class TTableWriter<T, std::enable_if_t<TIsBaseOf<Message, T>::Value>>
    : public TTableWriter<Message>
{
public:
    using TRowType = T;
    using TBase = TTableWriter<Message>;

    explicit TTableWriter(::TIntrusivePtr<IProtoWriterImpl> writer)
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

template <class T, class = std::enable_if_t<TIsBaseOf<Message, T>::Value>>
struct TWriterCreator
{
    static TTableWriterPtr<T> Create(::TIntrusivePtr<IProtoWriterImpl> writer)
    {
        return new TTableWriter<T>(writer);
    }
};

template <class T>
inline TTableWriterPtr<T> IIOClient::CreateTableWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    TAutoPtr<T> prototype(new T);
    return TWriterCreator<T>::Create(CreateProtoWriter(path, options, prototype.Get()));
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
