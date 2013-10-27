#pragma once

#include "public.h"

#include <core/misc/property.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryFragment
{
public:
    explicit TQueryFragment(TQueryContextPtr context, const TOperator* head = nullptr);
    ~TQueryFragment();

    DEFINE_BYVAL_RO_PROPERTY(TQueryContextPtr, Context);
    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Head);

};

TQueryFragment PrepareQueryFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

