#include "watch_log.h"

#include "object_reflection.h"

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void ValidateSelectors(
    const THashSet<TString>& selectors,
    const IObjectTypeHandler* typeHandler,
    bool abortOnFail)
{
    YT_VERIFY(typeHandler);
    for (const auto& selector: selectors) {
        auto resolveResult = ResolveAttribute(
            typeHandler,
            selector,
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        if (const auto* scalarSchema = resolveResult.Attribute->TryAsScalar();
            scalarSchema != nullptr && scalarSchema->IsAggregated())
        {
            if (abortOnFail) {
                YT_ABORT();
            } else {
                THROW_ERROR_EXCEPTION("Watch log selector %v cannot be an aggregated attribute", selector);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
