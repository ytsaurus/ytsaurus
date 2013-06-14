#include "stdafx.h"
#include "id_generator.h"
#include "serialize.h"

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

void TIdGenerator::Save(TStreamSaveContext& context) const
{
    ui64 current = static_cast<ui64>(Current);
    NYT::Save(context, current);
}

void TIdGenerator::Load(TStreamLoadContext& context)
{
    ui64 current = NYT::Load<ui64>(context);
    Current = static_cast<intptr_t>(current);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

