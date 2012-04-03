#include "stdafx.h"

#include "sync_writer.h"
#include "validating_writer.h"

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSyncValidatingAdaptor::TSyncValidatingAdaptor(TValidatingWriter* writer)
    : ValidatingWriter(writer)
{ }

void TSyncValidatingAdaptor::Open()
{
    Sync(ValidatingWriter.Get(), &TValidatingWriter::AsyncOpen);
}

void TSyncValidatingAdaptor::Write(const TColumn& column, TValue value)
{
    ValidatingWriter->Write(column, value);
}

void TSyncValidatingAdaptor::EndRow()
{
    Sync(ValidatingWriter.Get(), &TValidatingWriter::AsyncEndRow);
}

void TSyncValidatingAdaptor::Close()
{
    Sync(ValidatingWriter.Get(), &TValidatingWriter::AsyncClose);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
