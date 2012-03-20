#include "stdafx.h"

#include "sync_writer.h"
#include "validating_writer.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSyncWriter::TSyncWriter(TValidatingWriter* writer)
    : ValidatingWriter(writer)
{ }

void TSyncWriter::Open()
{
    Sync(ValidatingWriter.Get(), &TValidatingWriter::AsyncOpen);
}

void TSyncWriter::Write(const TColumn& column, TValue value)
{
    ValidatingWriter->Write(column, value);
}

void TSyncWriter::EndRow()
{
    Sync(ValidatingWriter.Get(), &TValidatingWriter::AsyncEndRow);
}

void TSyncWriter::Close()
{
    Sync(ValidatingWriter.Get(), &TValidatingWriter::AsyncClose);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
