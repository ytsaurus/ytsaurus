#pragma once

#include "fwd.h"

#include "client_method_options.h"
#include "common.h"
#include "format.h"
#include "node.h"
#include "mpl.h"

#include <contrib/libs/protobuf/message.h>

#include <statbox/ydl/runtime/cpp/gen_support/traits.h>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/generic/yexception.h>
#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TYdlGenericRowType
{ };

} // namespace NDetail

template<class ... TYdlRowTypes>
class TYdlOneOf
{
    static_assert((NYdl::TIsYdlGenerated<TYdlRowTypes>::value && ...), "Template parameters can only be YDL types");
};

////////////////////////////////////////////////////////////////////////////////

struct INodeReaderImpl;
struct IYaMRReaderImpl;
struct IProtoReaderImpl;
struct INodeWriterImpl;
struct IYaMRWriterImpl;
struct IProtoWriterImpl;

// These are temporary implementation details so you should not depend on this code
using IYdlReaderImpl = INodeReaderImpl;
using IYdlWriterImpl = INodeWriterImpl;

////////////////////////////////////////////////////////////////////////////////

class TIOException
    : public yexception
{ };

///////////////////////////////////////////////////////////////////////////////

class IFileReader
    : public TThrRefBase
    , public IInputStream
{ };

class IFileWriter
    : public TThrRefBase
    , public IOutputStream
{ };

////////////////////////////////////////////////////////////////////////////////

class TRawTableReader
    : public TThrRefBase
    , public IInputStream
{
public:
    // Retries table read starting from the specified rangeIndex and rowIndex.
    // If rowIndex is empty then entire last request will be retried.
    // Otherwise the request will be modified to retrieve table data starting from the specified
    // rangeIndex and rowIndex.
    // The method returns 'true' on successful request retry. If it returns 'false' then
    // the error is fatal and Retry() shouldn't be called any more.
    // After successful retry the user should reset rangeIndex / rowIndex values and read new ones
    // from the stream
    virtual bool Retry(
        const TMaybe<ui32>& rangeIndex,
        const TMaybe<ui64>& rowIndex) = 0;

    // Resets retry count to the initial value.
    virtual void ResetRetries() = 0;

    // Returns 'true' if the input stream may contain table ranges.
    // The TRawTableReader user is responsible to track active range index in this case
    // in order to pass it to Retry().
    virtual bool HasRangeIndices() const = 0;
};

class TRawTableWriter
    : public TThrRefBase
    , public IOutputStream
{
public:
    // Should be called after complete record is written.
    // When this method is called TRowTableWriter checks its buffer size
    // and if it is full it sends data to YT.
    // NOTE: TRawTableWriter never sends partial records to YT (due to retries).

    virtual void NotifyRowEnd() = 0;

    virtual void Abort()
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class>
class TTableReader
    : public TThrRefBase
{
public:
    const T& GetRow() const; // may be a template function
    T MoveRow();             // may be a template function
    void MoveRow(T* result); // may be a template function
    bool IsValid() const;
    ui32 GetTableIndex() const;
    ui32 GetRangeIndex() const;
    ui64 GetRowIndex() const;
    void Next();
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class>
class TTableRangesReader
    : public TThrRefBase
{
public:
    TTableReader<T>& GetRange();
    bool IsValid() const;
    void Next();
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class>
class TTableWriter
    : public TThrRefBase
{
public:
    void Abort(); // stop writing without flushing data (e.g. before aborting parent transaction)
    void AddRow(const T& row); // may be a template function
    void Finish();
};

////////////////////////////////////////////////////////////////////////////////

struct TYaMRRow
{
    TStringBuf Key;
    TStringBuf SubKey;
    TStringBuf Value;
};

////////////////////////////////////////////////////////////////////////////////

class IIOClient
{
public:
    virtual IFileReaderPtr CreateFileReader(
        const TRichYPath& path,
        const TFileReaderOptions& options = TFileReaderOptions()) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const TRichYPath& path,
        const TFileWriterOptions& options = TFileWriterOptions()) = 0;

    template <class T>
    TTableReaderPtr<T> CreateTableReader(
        const TRichYPath& path,
        const TTableReaderOptions& options = TTableReaderOptions());

    template <class T>
    TTableWriterPtr<T> CreateTableWriter(
        const TRichYPath& path,
        const TTableWriterOptions& options = TTableWriterOptions());

    virtual TTableWriterPtr<::google::protobuf::Message> CreateTableWriter(
        const TRichYPath& path,
        const ::google::protobuf::Descriptor& descriptor,
        const TTableWriterOptions& options = TTableWriterOptions()) = 0;

    virtual TRawTableReaderPtr CreateRawReader(
        const TRichYPath& path,
        const TFormat& format,
        const TTableReaderOptions& options = TTableReaderOptions()) = 0;

    virtual TRawTableWriterPtr CreateRawWriter(
        const TRichYPath& path,
        const TFormat& format,
        const TTableWriterOptions& options = TTableWriterOptions()) = 0;

    //
    // Read blob table.
    // https://wiki.yandex-team.ru/yt/userdoc/blob_tables/
    //
    // Blob table is a table that stores a number of blobs. Blobs are sliced into parts of the same size (maybe except of last part).
    // Those parts are stored in the separate rows.
    //
    // Blob table have constaints on its schema.
    //  - There must be columns that identify blob (blob id columns). That columns might be of any type.
    //  - There must be a column of int64 type that identify part inside the blob (this column is called `part index`).
    //  - There must be a column of string type that stores actual data (this column is called `data column`).
    virtual IFileReaderPtr CreateBlobTableReader(
        const TYPath& path,
        const TKey& blobId,
        const TBlobTableReaderOptions& options = TBlobTableReaderOptions()) = 0;

private:
    virtual ::TIntrusivePtr<INodeReaderImpl> CreateNodeReader(
        const TRichYPath& path, const TTableReaderOptions& options) = 0;

    virtual ::TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(
        const TRichYPath& path, const TTableReaderOptions& options) = 0;

    virtual ::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
        const TRichYPath& path,
        const TTableReaderOptions& options,
        const ::google::protobuf::Message* prototype) = 0;

    virtual ::TIntrusivePtr<IYdlReaderImpl> CreateYdlReader(
        const TRichYPath& /*path*/, const TTableReaderOptions& /*options*/)
    {
        Y_FAIL("Uimplemented");
    }

    virtual ::TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(
        const TRichYPath& path, const TTableWriterOptions& options) = 0;

    virtual ::TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(
        const TRichYPath& path, const TTableWriterOptions& options) = 0;

    virtual ::TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(
        const TRichYPath& path,
        const TTableWriterOptions& options,
        const ::google::protobuf::Message* prototype) = 0;

    virtual ::TIntrusivePtr<IYdlWriterImpl> CreateYdlWriter(
        const TRichYPath& /*path*/, const TTableWriterOptions& /*options*/)
    {
        Y_FAIL("Uimplemented");
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateTableReader(
    IInputStream* stream,
    const TTableReaderOptions& options = TTableReaderOptions());

template <>
TTableReaderPtr<TNode> CreateTableReader<TNode>(
    IInputStream* stream, const TTableReaderOptions& options);

template <>
TTableReaderPtr<TYaMRRow> CreateTableReader<TYaMRRow>(
    IInputStream* stream, const TTableReaderOptions& options);

namespace NDetail {

::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
    IInputStream* stream,
    const TTableReaderOptions& options,
    const ::google::protobuf::Descriptor* descriptor);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateConcreteProtobufReader(TTableReader<Message>* reader);
template <typename T>
TTableReaderPtr<T> CreateConcreteProtobufReader(const TTableReaderPtr<Message>& reader);

template <typename T>
TTableReaderPtr<Message> CreateGenericProtobufReader(TTableReader<T>* reader);
template <typename T>
TTableReaderPtr<Message> CreateGenericProtobufReader(const TTableReaderPtr<T>& reader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define IO_INL_H_
#include "io-inl.h"
#undef IO_INL_H_

