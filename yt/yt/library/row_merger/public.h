#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NRowMerger {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowMerger;
class TUnversionedRowMerger;
class TSamplingRowMerger;

struct IVersionedRowMerger;

DECLARE_REFCOUNTED_STRUCT(TNestedRowDiscardPolicy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowMerger
