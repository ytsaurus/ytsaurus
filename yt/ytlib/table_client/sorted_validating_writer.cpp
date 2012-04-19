#include "stdafx.h"
#include "sorted_validating_writer.h"

#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSortedValidatingWriter::TSortedValidatingWriter(
    const TSchema& schema,
    IAsyncBlockWriter* writer)
    : TValidatingWriter(schema, writer)
{
    PreviousKey.assign(Schema.KeyColumns().size(), Stroka());
    Attributes.set_sorted(true);
}

TAsyncError TSortedValidatingWriter::AsyncEndRow()
{
    if (PreviousKey > CurrentKey) {
        ythrow yexception() << Sprintf(
            "Invalid sorting. Current key %s is greater than previous %s.",
            ~JoinToString(CurrentKey),
            ~JoinToString(PreviousKey));
    }
    PreviousKey = CurrentKey;

    return TValidatingWriter::AsyncEndRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
