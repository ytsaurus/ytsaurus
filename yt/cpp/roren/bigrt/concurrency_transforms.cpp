#include "concurrency_transforms.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

NRoren::TStartConcurrencyBlockTransform StartConcurrencyBlock(const TSerializedBlockConfig& config)
{
    return TStartConcurrencyBlockTransform{config};
}

NRoren::TStartConcurrencyBlockTransform StartConcurrencyBlock(const TConcurrentBlockConfig& config)
{
    return TStartConcurrencyBlockTransform{config};
}

////////////////////////////////////////////////////////////////////////////////

TStartConcurrencyBlockTransform::TStartConcurrencyBlockTransform(NPrivate::TConcurrencyBlockConfig config)
    : Config_(std::move(config))
{ }

////////////////////////////////////////////////////////////////////////////////

TTypeTag<NPrivate::TExecutionBlockConfig> NPrivate::ExecutionBlockConfigTag{"execution_block_config_tag"};

} // namespace NRoren::NPrivate
