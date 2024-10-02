#pragma once
#ifndef TRANSACTION_WRAPPER_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_wrapper.h"
// For the sake of sane code completion.
#include "transaction_wrapper.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequestCommonOptions>
std::optional<bool> AllowFullScanFromOptions(const TRequestCommonOptions& options) 
{
    return options.has_allow_full_scan() ? std::make_optional(options.allow_full_scan()) : std::nullopt;
}
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
