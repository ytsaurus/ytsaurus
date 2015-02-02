#pragma once

#include "public.h"

#include <core/yson/public.h>
#include <core/misc/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct IParser
    : public TNonCopyable
{
    virtual ~IParser()
    { }

    virtual void Read(const TStringBuf& data) = 0;
    virtual void Finish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

void Parse(TInputStream* input, IParser* parser);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
