#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/range.h>
#include <yt/core/misc/ref.h>

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
    void WriteSchemafulRow(
        NTableClient::TUnversionedRow row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);

    void WriteUnversionedRowset(
        const TRange<NTableClient::TUnversionedRow>& rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteSchemafulRowset(
        const TRange<NTableClient::TUnversionedRow>& rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    NTableClient::ISchemafulWriterPtr CreateSchemafulRowsetWriter(
        const NTableClient::TTableSchema& schema);

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
    using TSchemaData = std::vector<ui32>;

    explicit TWireProtocolReader(const TSharedRef& data);
    ~TWireProtocolReader();

    bool IsFinished() const;
    TIterator GetBegin() const;
    TIterator GetEnd() const;

    TIterator GetCurrent() const;
    void SetCurrent(TIterator);

    TSharedRef Slice(TIterator begin, TIterator end);

    EWireProtocolCommand ReadCommand();

    NTableClient::TTableSchema ReadTableSchema();

    void ReadMessage(::google::protobuf::MessageLite* message);

    NTableClient::TUnversionedRow ReadUnversionedRow();
    NTableClient::TUnversionedRow ReadSchemafulRow(const TSchemaData& schemaData);
    TSharedRange<NTableClient::TUnversionedRow> ReadUnversionedRowset();
    TSharedRange<NTableClient::TUnversionedRow> ReadSchemafulRowset(const TSchemaData& schemaData);

    NTableClient::ISchemafulReaderPtr CreateSchemafulRowsetReader(
        const NTableClient::TTableSchema& schema);

    static TSchemaData GetSchemaData(
        const NTableClient::TTableSchema& schema,
        const NTableClient::TColumnFilter& filter);
    static TSchemaData GetSchemaData(const NTableClient::TTableSchema& schema);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    class TSchemafulRowsetReader;

    const TIntrusivePtr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

