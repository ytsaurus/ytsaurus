#include "evaluation_helpers.h"

#include "program.h"

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NQueryClient::NPortable {

////////////////////////////////////////////////////////////////////////////////

TCGExpressionImage CreateExpressionImage(TExpressionProgram program)
{
    return TCGExpressionImage(
        BIND_NO_PROPAGATE([program = std::move(program)] (
            TRange<TPIValue> /*literalValues*/,
            TRange<void*> /*opaqueData*/,
            TRange<size_t> /*opaqueDataSizes*/,
            TValue* result,
            TRange<TValue> inputRow,
            const TRowBufferPtr& rowBuffer,
            NWebAssembly::IWebAssemblyCompartment* /*compartment*/) {
            TCompactVector<TValue, 32> scratch(program.GetScratchValueCount());
            program.Evaluate(result, inputRow, scratch, rowBuffer);
        }),
        /*compartment*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
