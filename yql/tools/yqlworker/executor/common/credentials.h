#pragma once

#include <yql/essentials/core/credentials/yql_credentials.h>

namespace NYql {

void ParseCredentials(TStringBuf authData, TCredentials& credentials);

} // namespace NYql
