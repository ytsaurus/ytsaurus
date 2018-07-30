#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/schemaful_writer.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/range.h>
#include <yt/core/misc/ref.h>

#include <yt/core/compression/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

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

    ((VersionedLookupRows)(2))
    // Finds rows with given keys and fetches their components.
    //
    // Input:
    //   * TReqLookupRows
    //   * Unversioned rowset containing N keys
    //
    // Output:
    //   * N versioned rows

    // Write commands:

    ((WriteRow)(100))
    // Inserts a new row or completely replaces an existing one with matching key.
    //
    // Input:
    //   * Unversioned row
    // Output:
    //   None

    ((DeleteRow)(101))
    // Deletes a row with a given key, if it exists.
    //
    // Input:
    //   * Key
    // Output:
    //   None

    ((VersionedWriteRow)(102))
    // Writes a versioned row (possibly inserting new values and/or delete timestamps).
    // Currently only used by replicator.
    //
    // Input:
    //   * Versioned row
    // Output:
    //   None
);

////////////////////////////////////////////////////////////////////////////////

//! Builds wire-encoded stream.
class TWireProtocolWriter
{
public:
    TWireProtocolWriter();
    TWireProtocolWriter(const TWireProtocolWriter&) = delete;
    TWireProtocolWriter(TWireProtocolWriter&&) = delete;
    ~TWireProtocolWriter();

    size_t GetByteSize() const;

    void WriteCommand(EWireProtocolCommand command);

    void WriteTableSchema(const NTableClient::TTableSchema& schema);

    void WriteMessage(const ::google::protobuf::MessageLite& message);

    size_t WriteUnversionedRow(
        NTableClient::TUnversionedRow row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    size_t WriteSchemafulRow(
        NTableClient::TUnversionedRow row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    size_t WriteVersionedRow(
        NTableClient::TVersionedRow row);

    void WriteUnversionedValueRange(
        TRange<NTableClient::TUnversionedValue> valueRange,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);

    void WriteUnversionedRowset(
        TRange<NTableClient::TUnversionedRow> rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteSchemafulRowset(
        TRange<NTableClient::TUnversionedRow> rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);
    void WriteVersionedRowset(
        TRange<NTableClient::TVersionedRow> rowset);

    template <class TRow>
    inline void WriteRowset(TRange<TRow> rowset);

    std::vector<TSharedRef> Finish();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

template <>
inline void TWireProtocolWriter::WriteRowset<NTableClient::TUnversionedRow>(
    TRange<NTableClient::TUnversionedRow> rowset)
{
    return WriteUnversionedRowset(rowset);
}

template <>
inline void TWireProtocolWriter::WriteRowset<NTableClient::TVersionedRow>(
    TRange<NTableClient::TVersionedRow> rowset)
{
    return WriteVersionedRowset(rowset);
}

////////////////////////////////////////////////////////////////////////////////

//! Reads wire-encoded stream.
/*!
 *  All |ReadXXX| methods obey the following convention.
 *  Rows are captured by the row buffer passed in ctor.
 *  Values are either captured or not depending on |deep| argument.
 */
class TWireProtocolReader
{
public:
    using TIterator = const char*;

    //! Initializes the instance.
    /*!
     *  If #rowBuffer is null, a default one is created.
     */
    TWireProtocolReader(
        const TSharedRef& data,
        NTableClient::TRowBufferPtr rowBuffer = NTableClient::TRowBufferPtr());
    TWireProtocolReader(const TWireProtocolReader&) = delete;
    TWireProtocolReader(TWireProtocolReader&&) = delete;
    ~TWireProtocolReader();

    const NTableClient::TRowBufferPtr& GetRowBuffer() const;

    bool IsFinished() const;
    TIterator GetBegin() const;
    TIterator GetEnd() const;

    TIterator GetCurrent() const;
    void SetCurrent(TIterator);

    TSharedRef Slice(TIterator begin, TIterator end);

    EWireProtocolCommand ReadCommand();

    NTableClient::TTableSchema ReadTableSchema();

    void ReadMessage(::google::protobuf::MessageLite* message);

    NTableClient::TUnversionedRow ReadUnversionedRow(bool deep);
    NTableClient::TUnversionedRow ReadSchemafulRow(const TSchemaData& schemaData, bool deep);
    NTableClient::TVersionedRow ReadVersionedRow(const TSchemaData& schemaData, bool deep);

    TSharedRange<NTableClient::TUnversionedRow> ReadUnversionedRowset(bool deep);
    TSharedRange<NTableClient::TUnversionedRow> ReadSchemafulRowset(const TSchemaData& schemaData, bool deep);
    TSharedRange<NTableClient::TVersionedRow> ReadVersionedRowset(const TSchemaData& schemaData, bool deep);

    template <class TRow>
    inline TSharedRange<TRow> ReadRowset(const TSchemaData& schemaData, bool deep);

    static TSchemaData GetSchemaData(
        const NTableClient::TTableSchema& schema,
        const NTableClient::TColumnFilter& filter);
    static TSchemaData GetSchemaData(const NTableClient::TTableSchema& schema);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;

};

template <>
inline TSharedRange<NTableClient::TUnversionedRow> TWireProtocolReader::ReadRowset<NTableClient::TUnversionedRow>(
    const TSchemaData& schemaData,
    bool deep)
{
    return ReadUnversionedRowset(deep);
}

template <>
inline TSharedRange<NTableClient::TVersionedRow> TWireProtocolReader::ReadRowset<NTableClient::TVersionedRow>(
    const TSchemaData& schemaData,
    bool deep)
{
    return ReadVersionedRowset(schemaData, deep);
}

////////////////////////////////////////////////////////////////////////////////

struct IWireProtocolRowsetReader
    : public NTableClient::ISchemafulReader
{ };

DEFINE_REFCOUNTED_TYPE(IWireProtocolRowsetReader)

IWireProtocolRowsetReaderPtr CreateWireProtocolRowsetReader(
    const std::vector<TSharedRef>& compressedBlocks,
    NCompression::ECodec codecId,
    const NTableClient::TTableSchema& schema,
    bool isSchemaful,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct IWireProtocolRowsetWriter
    : public NTableClient::ISchemafulWriter
{
    virtual std::vector<TSharedRef> GetCompressedBlocks() = 0;
};

DEFINE_REFCOUNTED_TYPE(IWireProtocolRowsetWriter)

IWireProtocolRowsetWriterPtr CreateWireProtocolRowsetWriter(
    NCompression::ECodec codecId,
    size_t desiredUncompressedBlockSize,
    const NTableClient::TTableSchema& schema,
    bool isSchemaful,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

