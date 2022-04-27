#ifndef CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include context.h"
// For the sake of sane code completion.
#include "context.h"
#endif

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
void ISequoiaContext::WriteRow(const TRow& row)
{
    const auto& tableDescriptor = TRow::TTable::Get();
    WriteRow(tableDescriptor->GetType(), tableDescriptor->ToUnversionedRow(row, GetRowBuffer()));
}

template <class TRow>
void ISequoiaContext::DeleteRow(const TRow& row)
{
    const auto& tableDescriptor = TRow::TTable::Get();
    DeleteRow(tableDescriptor->GetType(), tableDescriptor->ToKey(row, GetRowBuffer()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
