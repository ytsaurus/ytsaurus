#pragma once

#include <ydb-cpp-sdk/type_switcher.h>

#include <string_view>

namespace NYdb {
inline namespace V3 {
namespace NIssue {

bool IsUtf8(const std::string_view& str);

}
}
}
