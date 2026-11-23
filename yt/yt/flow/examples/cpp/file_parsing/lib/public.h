#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TParseTask);
DECLARE_REFCOUNTED_STRUCT(TParseResult);
DECLARE_REFCOUNTED_STRUCT(TParsedFile);
DECLARE_REFCOUNTED_STRUCT(TFailedFile);

DECLARE_REFCOUNTED_STRUCT(TParseExecutorConfig);
DECLARE_REFCOUNTED_CLASS(TParseExecutor);

DECLARE_REFCOUNTED_STRUCT(TParseExecutorResourceParameters);
DECLARE_REFCOUNTED_CLASS(TParseExecutorResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
