#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void DumpPlanFragment(
    const TPlanFragment& fragment,
    TOutputStream& output,
    const Stroka& title = "");

void DumpPlanFragmentToFile(
    const TPlanFragment& fragment,
    const Stroka& name = "plan-fragment.dot",
    const Stroka& title = "");

void ViewPlanFragment(
    const TPlanFragment& fragment,
    const Stroka& title = "");

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

