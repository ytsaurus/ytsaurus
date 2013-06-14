#pragma once

#include "common.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe generator producing increasing sequence of numbers.
class TIdGenerator
{
public:
    TIdGenerator();

    ui64 Next();
    void Reset();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TAtomic Current;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
