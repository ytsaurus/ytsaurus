#pragma once

#include "public.h"
#include <ytlib/misc/common.h>

namespace NYT {
namespace NYTree {

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

void Parse(TInputStream* input, IYsonConsumer* consumer, IParser* parser);

////////////////////////////////////////////////////////////////////////////////


} // namespace NYTree
} // namespace NYT
