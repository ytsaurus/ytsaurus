#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/ref.h>
#include <core/misc/range.h>

#include <ytlib/new_table_client/public.h>

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

    void WriteTableSchema(const NVersionedTableClient::TTableSchema& schema);

    void WriteMessage(const ::google::protobuf::MessageLite& message);

    void WriteUnversionedRow(
        NVersionedTableClient::TUnversionedRow row,
        const NVersionedTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteUnversionedRow(
        const TRange<NVersionedTableClient::TUnversionedValue>& row,
        const NVersionedTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteUnversionedRowset(
        const TRange<NVersionedTableClient::TUnversionedRow>& rowset,
        const NVersionedTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    NVersionedTableClient::ISchemafulWriterPtr CreateSchemafulRowsetWriter();

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
    TIterator GetBegin() const;
    TIterator GetEnd() const;

    TIterator GetCurrent() const;
    void SetCurrent(TIterator);

    TSharedRef Slice(TIterator begin, TIterator end);

    EWireProtocolCommand ReadCommand();

    NVersionedTableClient::TTableSchema ReadTableSchema();

    void ReadMessage(::google::protobuf::MessageLite* message);

    NVersionedTableClient::TUnversionedRow ReadUnversionedRow();
    TSharedRange<NVersionedTableClient::TUnversionedRow> ReadUnversionedRowset();
    NVersionedTableClient::ISchemafulReaderPtr CreateSchemafulRowsetReader();

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    class TSchemafulRowsetReader;

    const TIntrusivePtr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

