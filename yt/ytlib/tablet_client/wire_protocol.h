#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/ref.h>
#include <core/misc/range.h>

#include <ytlib/table_client/public.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWireProtocolCommand,
    // Read commands:
    
    ((LookupRows)(1))
    // Finds rows with given keys and fetches their components.
    //
    // Input:
    //   * TReqLookupRows
    //   * Unversioned rowset containing N keys
    //
    // Output:
    //   * N unversioned rows


    // Write commands:

    ((WriteRow)(100))
    // Inserts a new row or completely replaces an existing one with matching key.
    //
    // Input:
    //   * TReqWriteRow
    //   * Unversioned row
    // Output:
    //   None

    ((DeleteRow)(101))
    // Deletes a row with a given key, if it exists.
    //
    // Input:
    //   * TReqDeleteRow
    //   * Key
    // Output:
    //   None


    // Rowset commands:
    ((RowsetChunk)(200))
    ((EndOfRowset)(201))

);

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter
    : private TNonCopyable
{
public:
    TWireProtocolWriter();
    ~TWireProtocolWriter();

    void WriteCommand(EWireProtocolCommand command);

    void WriteTableSchema(const NTableClient::TTableSchema& schema);

    void WriteMessage(const ::google::protobuf::MessageLite& message);

    void WriteUnversionedRow(
        NTableClient::TUnversionedRow row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteUnversionedRow(
        const TRange<NTableClient::TUnversionedValue>& row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteUnversionedRowset(
        const TRange<NTableClient::TUnversionedRow>& rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    NTableClient::ISchemafulWriterPtr CreateSchemafulRowsetWriter();

    std::vector<TSharedRef> Flush();

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    class TSchemafulRowsetWriter;

    const TIntrusivePtr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader
    : private TNonCopyable
{
public:
    using TIterator = const char*;

    explicit TWireProtocolReader(const TSharedRef& data);
    ~TWireProtocolReader();

    bool IsFinished() const;

    TIterator GetCurrent() const;
    void SetCurrent(TIterator);

    TSharedRef Slice(TIterator begin, TIterator end);

    EWireProtocolCommand ReadCommand();

    NTableClient::TTableSchema ReadTableSchema();

    void ReadMessage(::google::protobuf::MessageLite* message);

    NTableClient::TUnversionedRow ReadUnversionedRow();
    TSharedRange<NTableClient::TUnversionedRow> ReadUnversionedRowset();

    NTableClient::ISchemafulReaderPtr CreateSchemafulRowsetReader(
        const NTableClient::TTableSchema& schema);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    class TSchemafulRowsetReader;

    const TIntrusivePtr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

