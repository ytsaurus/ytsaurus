#pragma once

#include "common.h"
#include "node.h"
#include "mpl.h"

#include <contrib/libs/protobuf/message.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct INodeReaderImpl;
struct IYaMRReaderImpl;
struct IProtoReaderImpl;
struct INodeWriterImpl;
struct IYaMRWriterImpl;
struct IProtoWriterImpl;

////////////////////////////////////////////////////////////////////////////////

class TIOException
    : public yexception
{ };

///////////////////////////////////////////////////////////////////////////////

class IFileReader
    : public TThrRefBase
    , public TInputStream
{ };

using IFileReaderPtr = TIntrusivePtr<IFileReader>;

class IFileWriter
    : public TThrRefBase
    , public TOutputStream
{ };

using IFileWriterPtr = TIntrusivePtr<IFileWriter>;

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
class TTableReader
    : public TThrRefBase
{
public:
    const T& GetRow() const; // may be a template function
    bool IsValid() const;
    size_t GetTableIndex() const;
    void Next();
};

template <class T>
using TTableReaderPtr = TIntrusivePtr<TTableReader<T>>;

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
class TTableWriter
    : public TThrRefBase
{
public:
    void AddRow(const T& row); // may be a template function
    void Finish();
};

template <class T>
using TTableWriterPtr = TIntrusivePtr<TTableWriter<T>>;

////////////////////////////////////////////////////////////////////////////////

struct TYaMRRow
{
    Stroka Key;
    Stroka SubKey;
    Stroka Value;
};

using ::google::protobuf::Message;

////////////////////////////////////////////////////////////////////////////////

class IIOClient
{
public:
    virtual IFileReaderPtr CreateFileReader(const TRichYPath& path) = 0;

    virtual IFileWriterPtr CreateFileWriter(const TRichYPath& path) = 0;

    template <class T>
    TTableReaderPtr<T> CreateTableReader(const TRichYPath& path);

    template <class T>
    TTableWriterPtr<T> CreateTableWriter(const TRichYPath& path);

private:
    virtual TIntrusivePtr<INodeReaderImpl> CreateNodeReader(const TRichYPath& path) = 0;
    virtual TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(const TRichYPath& path) = 0;
    virtual TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(const TRichYPath& path) = 0;

    virtual TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(const TRichYPath& path) = 0;
    virtual TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(const TRichYPath& path) = 0;
    virtual TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(const TRichYPath& path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define IO_INL_H_
#include "io-inl.h"
#undef IO_INL_H_

