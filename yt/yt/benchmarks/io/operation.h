#pragma once

#include "iotest.h"
#include "configuration.h"

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

using TOperationGenerator = std::function<TOperation()>;

TOperationGenerator CreateOperationGenerator(TConfigurationPtr configuration, int threadIndex);
TOperationGenerator CreateRandomOperationGenerator(TConfigurationPtr configuration);
TOperationGenerator CreateSequentialOperationGenerator(TConfigurationPtr configuration);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
