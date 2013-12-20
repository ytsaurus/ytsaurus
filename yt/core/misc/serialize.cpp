#include "stdafx.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TStreamSaveContext::TStreamSaveContext()
    : Output_(nullptr)
{ }

TStreamSaveContext::TStreamSaveContext(TOutputStream* output)
    : Output_(output)
{ }

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContext::TStreamLoadContext()
    : Input_(nullptr)
{ }

TStreamLoadContext::TStreamLoadContext(TInputStream* input)
    : Input_(input)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

