#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/ref.h>

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
        const std::vector<NVersionedTableClient::TUnversionedValue>& row,
        const NVersionedTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteUnversionedRowset(
        const std::vector<NVersionedTableClient::TUnversionedRow>& rowset,
        const NVersionedTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    NVersionedTableClient::ISchemafulWriterPtr CreateSchemafulRowsetWriter();

    std::vector<TSharedRef> Flush();

private:
    class TImpl;
    class TSchemafulRowsetWriter;

    std::unique_ptr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader
{
public:
    explicit TWireProtocolReader(const TSharedRef& data);
    ~TWireProtocolReader();

    bool IsFinished() const;
    TSharedRef GetConsumedPart() const;
    TSharedRef GetRemainingPart() const;

    const char* GetCurrent() const;
    void SetCurrent(const char* current);

    EWireProtocolCommand ReadCommand();

    NVersionedTableClient::TTableSchema ReadTableSchema();

    void ReadMessage(::google::protobuf::MessageLite* message);

    NVersionedTableClient::TUnversionedRow ReadUnversionedRow();
    void ReadUnversionedRowset(std::vector<NVersionedTableClient::TUnversionedRow>* rowset);
    NVersionedTableClient::ISchemafulReaderPtr CreateSchemafulRowsetReader();

private:
    class TImpl;
    class TSchemafulRowsetReader;

    std::unique_ptr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

