#pragma once

#include "public.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    TYsonParser(
        IYsonConsumer* consumer,
        EYsonType type = EYsonType::Node,
        bool enableLinePositionInfo = false);

    ~TYsonParser();

    void Read(const TStringBuf& data);
    void Finish();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
