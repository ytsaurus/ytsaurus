#pragma once

#include "public.h"
#include "schema.h"
#include "block_writer.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriter
    : public virtual TRefCounted
{
public:
    TChunkWriter(TChunkWriterConfigPtr config, NChunkClient::IAsyncWriterPtr asyncWriter)

    TAsyncError Open(
        TNameTablePtr nameTable, 
        const TTableSchema& schema, 
        const TKeyColumns& keyColumns = TKeyColumns(),
        ERowSetType type = ERowsetType::Simple);

    void WriteValue(const TRowValue& value);
    bool EndRow(TTimestamp timestamp = NullTimestamp, bool deleted = false);

    TAsyncError GetReadyEvent();

    TAsyncError AsyncClose();

private:
    struct TColumnDescriptor {
        int IndexInBlock;
        EValueType Type;

        // Used for versioned rowsets.
        bool IsKeyPart;

        union {
            TStringBuf String;
            double Double;
            i64 Integer;
        } PreviousValue;
    };

    // Vector is indexed by input NameTable indexes.
    std::vector<TColumnDescriptor> ColumnDescriptors;

    TBlockWriter BlockWriter;

    NChunkClient::TEncodingWriterPtr EncodingWriter;

    std::vector<TSharedRef> PendingBlock;
    bool HasPendingBlock;

    bool IsNewKey;


};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
