#ifndef IO_ENGINE_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include io_engine_base.h"
// For the sake of sane code completion.
#include "io_engine_base.h"
#endif

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename... TParams>
IIOEnginePtr CreateIOEngine(const NYTree::INodePtr& ioConfig, TParams... params)
{
    auto config = New<typename T::TConfig>();
    config->SetDefaults();
    if (ioConfig) {
        config->Load(ioConfig);
    }

    return New<T>(std::move(config), std::forward<TParams>(params)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
