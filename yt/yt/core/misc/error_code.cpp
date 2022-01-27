#include "error_code.h"

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/format.h>

#include <util/string/split.h>

#include <util/system/type_name.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TErrorCodeRegistry* TErrorCodeRegistry::Get()
{
    return LeakySingleton<TErrorCodeRegistry>();
}

TErrorCodeRegistry::TErrorCodeInfo TErrorCodeRegistry::Get(int code)
{
    auto it = CodeToInfo_.find(code);
    if (it != CodeToInfo_.end()) {
        return it->second;
    }
    return {"NUnknown", Format("ErrorCode%v", code)};
}

void TErrorCodeRegistry::RegisterErrorCode(int code, const TErrorCodeInfo& errorCodeInfo)
{
    YT_VERIFY(CodeToInfo_.insert({code, errorCodeInfo}).second);
}

TString TErrorCodeRegistry::ParseNamespace(const std::type_info& errorCodeEnumTypeInfo)
{
    TString name;
    // Ensures that "EErrorCode" is found as a substring in the type name and stores the prefix before
    // the first occurrence into #name.
    YT_VERIFY(StringSplitter(
        TypeName(errorCodeEnumTypeInfo)).SplitByString("EErrorCode").Limit(2).TryCollectInto(&name, &std::ignore));
    // If the enum was declared directly in the global namespace, #name should be empty.
    // Otherwise, #name should end with "::".
    if (!name.empty()) {
        YT_VERIFY(name.EndsWith("::"));
        name.resize(name.size() - 2);
    }
    return name;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
