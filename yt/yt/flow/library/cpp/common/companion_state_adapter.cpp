#include "companion_state_adapter.h"

#include "input_context.h"
#include "state_provider.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

THashSet<TKey> ICompanionStateAdapter::ExtractKeys(const IInputContextPtr& input) const
{
    return NFlow::ExtractKeys(input);
}

////////////////////////////////////////////////////////////////////////////////

THashSet<TKey> ExtractJoinedStateKeys(
    const IJoinedStateKeyProvider& provider,
    const IInputContextPtr& input)
{
    return NFlow::ExtractKeys(
        input,
        provider.HasKeySchemaOverride() ? provider.GetKeySchema() : nullptr,
        provider.GetKeyProviderStreams(),
        provider.GetConverterCache());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
