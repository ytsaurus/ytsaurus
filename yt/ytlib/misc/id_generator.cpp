#include "stdafx.h"
#include "id_generator.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TIdGenerator::TIdGenerator()
    : Current(0)
{ }

ui64 TIdGenerator::Next()
{
    return AtomicIncrement(Current) - 1;
}

void TIdGenerator::Reset()
{
    AtomicSet(Current, 0);
}

void Save(TOutputStream* output, const TIdGenerator& generator)
{
    ui64 current = static_cast<i64>(generator.Current);
    ::Save(output, current);
}

void Load(TInputStream* input, TIdGenerator& generator)
{
    ui64 current;
    ::Load(input, current);
    generator.Current = static_cast<intptr_t>(current);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

