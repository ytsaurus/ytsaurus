#pragma once

#include "public.h"

#include <core/misc/property.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryFragment
{
public:
    explicit TQueryFragment(TQueryContextPtr context, TOperator* head = nullptr);
    ~TQueryFragment();

    DEFINE_BYVAL_RO_PROPERTY(TQueryContextPtr, Context);
    DEFINE_BYVAL_RW_PROPERTY(TOperator*, Head);

};

TQueryFragment PrepareQueryFragment(
    IPreparationHooks* context,
    const Stroka& source);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

