#pragma once

#include "public.h"

#include <ytlib/formats/format.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/blob_output.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TContextPreservingInput
    : public TRefCounted
{
public:
    TContextPreservingInput(
        NVersionedTableClient::ISchemalessMultiChunkReaderPtr reader,
        const NFormats::TFormat& format,
        bool enableTableSwitch,
        bool enableKeySwitch);

    void PipeReaderToOutput(TOutputStream* outputStream);

    TBlob GetContext() const;

private:
    NVersionedTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    NVersionedTableClient::ISchemalessMultiSourceWriterPtr Writer_;

    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;

    NYson::IYsonConsumer* Consumer_;

    void WriteRows(
        const std::vector<NVersionedTableClient::TUnversionedRow>& rows,
        TOutputStream* outputStream);
};

DEFINE_REFCOUNTED_TYPE(TContextPreservingInput);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NJobProxy
