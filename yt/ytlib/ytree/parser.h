#pragma once

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

} // namespace NYTree
} // namespace NYT
