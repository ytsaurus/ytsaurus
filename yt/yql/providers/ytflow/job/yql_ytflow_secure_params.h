#pragma once

#include <memory>

namespace NYql::NUdf {

class ISecureParamsProvider;

} // namespace NYql::NUdf

namespace NYql::NYtflow {

std::unique_ptr<NUdf::ISecureParamsProvider> CreateSecureParamsProvider();

} // namespace NYql::NYtflow
