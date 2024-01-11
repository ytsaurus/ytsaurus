#include "helpers.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow BuildNativeKey(TKey key)
{
    NTableClient::TUnversionedOwningRowBuilder builder;
    builder.AddValue(NTableClient::MakeUnversionedInt64Value(key));
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
