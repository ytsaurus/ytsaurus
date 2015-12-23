#pragma once

#include <yt/core/misc/nullable.h>

#include <util/generic/stroka.h>

namespace NYT {
namespace NJobProberClient {

TNullable<int> FindSignalIdBySignalName(const Stroka& signalName);
void ValidateSignalName(const Stroka& signalName);

} // namespace NJobProberClient
} // NYT
