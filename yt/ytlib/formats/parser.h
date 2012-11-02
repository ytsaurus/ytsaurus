#pragma once

#include "public.h"

#include <ytlib/yson/public.h>
#include <ytlib/misc/common.h>

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

void Parse(TInputStream* input, NYson::IYsonConsumer* consumer, IParser* parser);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
