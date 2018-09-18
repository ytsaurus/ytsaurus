#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TStreamSaveContext::TStreamSaveContext()
    : Output_(nullptr)
{ }

TStreamSaveContext::TStreamSaveContext(IOutputStream* output)
    : Output_(output)
{ }

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContext::TStreamLoadContext()
    : Input_(nullptr)
{ }

TStreamLoadContext::TStreamLoadContext(IInputStream* input)
    : Input_(input)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

