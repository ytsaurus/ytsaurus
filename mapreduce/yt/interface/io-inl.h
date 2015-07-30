#pragma once

#ifndef IO_INL_H_
#error "Direct inclusion of this file is not allowed, use io.h"
#endif
#undef IO_INL_H_

#include <util/stream/length.h>
#include <util/generic/typetraits.h>

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
struct TRowTraits<T, TEnableIf<TIsBaseOf<Message, T>::Value>>
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
    virtual size_t GetTableIndex() const = 0;

    virtual void NextKey() = 0;
};

struct INodeReaderImpl
    : public IReaderImplBase
{
    virtual const TNode& GetRow() const = 0;
};

struct IYaMRReaderImpl
    : public IReaderImplBase
{
    virtual const TYaMRRow& GetRow() const = 0;
};

struct IProtoReaderImpl
    : public IReaderImplBase
{
    virtual void SkipRow() = 0;
    virtual void ReadRow(Message* row) = 0; // currently proto over yson
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TTableReaderBase
    : public TThrRefBase
{
public:
    using TRowType = T;
    using IReaderImpl = typename TRowTraits<T>::IReaderImpl;

    explicit TTableReaderBase(TIntrusivePtr<IReaderImpl> reader)
        : Reader_(reader)
    { }

    const T& GetRow() const
    {
        return Reader_->GetRow();
    }

    bool IsValid() const
    {
        return Reader_->IsValid();
    }

    void Next()
    {
        Reader_->Next();
    }

    size_t GetTableIndex() const
    {
        return Reader_->GetTableIndex();
    }

private:
    TIntrusivePtr<IReaderImpl> Reader_;
};

template <>
class TTableReader<TNode>
    : public TTableReaderBase<TNode>
{
public:
    using TBase = TTableReaderBase<TNode>;
    using TBase::TBase;
};

template <>
class TTableReader<TYaMRRow>
    : public TTableReaderBase<TYaMRRow>
{
public:
    using TBase = TTableReaderBase<TYaMRRow>;
    using TBase::TBase;
};

template <>
class TTableReader<Message>
    : public TThrRefBase
{
public:
    using TRowType = Message;

    explicit TTableReader(TIntrusivePtr<IProtoReaderImpl> reader)
        : Reader_(reader)
    { }

    template <class U, TEnableIf<TIsBaseOf<Message, U>::Value>* = nullptr>
    const U& GetRow() const
    {
        if (CachedRow_) {
            return *dynamic_cast<U*>(CachedRow_.Get());
        }
        CachedRow_.Reset(new U);
        Reader_->ReadRow(CachedRow_.Get());
        return *dynamic_cast<U*>(CachedRow_.Get());
    }

    bool IsValid() const
    {
        return Reader_->IsValid();
    }

    void Next()
    {
        if (!CachedRow_.Get()) {
            Reader_->SkipRow();
        }
        Reader_->Next();
        CachedRow_.Reset(nullptr);
    }

    size_t GetTableIndex() const
    {
        return Reader_->GetTableIndex();
    }

private:
    TIntrusivePtr<IProtoReaderImpl> Reader_;
    mutable THolder<Message> CachedRow_;
};

template <class T>
class TTableReader<T, TEnableIf<TIsBaseOf<Message, T>::Value>>
    : public TTableReader<Message>
{
public:
    using TRowType = T;
    using TBase = TTableReader<Message>;
    using TBase::TBase;

    const T& GetRow() const
    {
        return TBase::GetRow<T>();
    }
};

template <>
inline TTableReaderPtr<TNode> IIOClient::CreateTableReader<TNode>(const TRichYPath& path)
{
    return new TTableReader<TNode>(CreateNodeReader(path));
}

template <>
inline TTableReaderPtr<TYaMRRow> IIOClient::CreateTableReader<TYaMRRow>(const TRichYPath& path)
{
    return new TTableReader<TYaMRRow>(CreateYaMRReader(path));
}

template <class T, class = TEnableIf<TIsBaseOf<Message, T>::Value>>
struct TReaderCreator
{
    static TTableReaderPtr<T> Create(TIntrusivePtr<IProtoReaderImpl> reader)
    {
        return new TTableReader<T>(reader);
    }
};

template <class T>
inline TTableReaderPtr<T> IIOClient::CreateTableReader(const TRichYPath& path)
{
    return TReaderCreator<T>::Create(CreateProtoReader(path));
}

////////////////////////////////////////////////////////////////////////////////

struct IWriterImplBase
    : public TThrRefBase
{
    virtual void Finish() = 0;
};

struct INodeWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TNode& row, size_t tableIndex = 0) = 0;
};

struct IYaMRWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const TYaMRRow& row, size_t tableIndex = 0) = 0;
};

struct IProtoWriterImpl
    : public IWriterImplBase
{
    virtual void AddRow(const Message& row, size_t tableIndex = 0) = 0;
};

template <class T>
class TTableWriterBase
    : public TThrRefBase
{
public:
    using TRowType = T;
    using IWriterImpl = typename TRowTraits<T>::IWriterImpl;

    explicit TTableWriterBase(TIntrusivePtr<IWriterImpl> writer)
        : Writer_(writer)
    { }

    ~TTableWriterBase()
    {
        try {
            Finish();
        } catch (...) {
            // no guarantees
        }
    }

    void AddRow(const T& row, size_t tableIndex = 0)
    {
        Writer_->AddRow(row, tableIndex);
    }

    void Finish()
    {
        Writer_->Finish();
    }

private:
    TIntrusivePtr<IWriterImpl> Writer_;
};

template <>
class TTableWriter<TNode>
    : public TTableWriterBase<TNode>
{
public:
    using TBase = TTableWriterBase<TNode>;
    using TBase::TBase;
};

template <>
class TTableWriter<TYaMRRow>
    : public TTableWriterBase<TYaMRRow>
{
public:
    using TBase = TTableWriterBase<TYaMRRow>;
    using TBase::TBase;
};

template <>
class TTableWriter<Message>
    : public TThrRefBase
{
public:
    using TRowType = Message;

    explicit TTableWriter(TIntrusivePtr<IProtoWriterImpl> writer)
        : Writer_(writer)
    { }

    ~TTableWriter()
    {
        try {
            Finish();
        } catch (...) {
            // no guarantees
        }
    }

    template <class U, TEnableIf<TIsBaseOf<Message, U>::Value>* = nullptr>
    void AddRow(const U& row, size_t tableIndex = 0)
    {
        Writer_->AddRow(row, tableIndex);
    }

    void Finish()
    {
        Writer_->Finish();
    }

private:
    TIntrusivePtr<IProtoWriterImpl> Writer_;
};

template <class T>
class TTableWriter<T, TEnableIf<TIsBaseOf<Message, T>::Value>>
    : public TTableWriter<Message>
{
public:
    using TRowType = T;
    using TBase = TTableWriter<Message>;
    using TBase::TBase;

    void AddRow(const T& row, size_t tableIndex = 0)
    {
        TBase::AddRow<T>(row, tableIndex);
    }
};

template <>
inline TTableWriterPtr<TNode> IIOClient::CreateTableWriter<TNode>(const TRichYPath& path)
{
    return new TTableWriter<TNode>(CreateNodeWriter(path));
}

template <>
inline TTableWriterPtr<TYaMRRow> IIOClient::CreateTableWriter<TYaMRRow>(const TRichYPath& path)
{
    return new TTableWriter<TYaMRRow>(CreateYaMRWriter(path));
}

template <class T, class = TEnableIf<TIsBaseOf<Message, T>::Value>>
struct TWriterCreator
{
    static TTableWriterPtr<T> Create(TIntrusivePtr<IProtoWriterImpl> writer)
    {
        return new TTableWriter<T>(writer);
    }
};

template <class T>
inline TTableWriterPtr<T> IIOClient::CreateTableWriter(const TRichYPath& path)
{
    return TWriterCreator<T>::Create(CreateProtoWriter(path));
}

////////////////////////////////////////////////////////////////////////////////

}
