#include "error_code.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/format.h>

#include <util/string/split.h>

#include <util/system/type_name.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Remove this once we find all duplicate error codes.
static NLogging::TLogger GetLogger()
{
    static NLogging::TLogger logger("ErrorCode");
    return logger;
}

////////////////////////////////////////////////////////////////////////////////

bool TErrorCodeRegistry::TErrorCodeInfo::operator==(const TErrorCodeInfo& rhs) const
{
    return Namespace == rhs.Namespace && Name == rhs.Name;
}

TErrorCodeRegistry* TErrorCodeRegistry::Get()
{
    return LeakySingleton<TErrorCodeRegistry>();
}

TErrorCodeRegistry::TErrorCodeInfo TErrorCodeRegistry::Get(int code) const
{
    auto it = CodeToInfo_.find(code);
    if (it != CodeToInfo_.end()) {
        return it->second;
    }
    return {"NUnknown", Format("ErrorCode%v", code)};
}

THashMap<int, TErrorCodeRegistry::TErrorCodeInfo> TErrorCodeRegistry::GetAll() const
{
    return CodeToInfo_;
}

void TErrorCodeRegistry::RegisterErrorCode(int code, const TErrorCodeInfo& errorCodeInfo)
{
    if (!CodeToInfo_.insert({code, errorCodeInfo}).second) {
        // TODO(achulkov2): Deal with duplicate TransportError in NRpc and NBus.
        if (code == 100) {
            return;
        }
        auto Logger = GetLogger();
        YT_LOG_FATAL(
            "Duplicate error code (Code: %v, StoredCodeInfo: %v, NewCodeInfo: %v)",
            code,
            CodeToInfo_[code],
            errorCodeInfo);
    }
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

TString ToString(const TErrorCodeRegistry::TErrorCodeInfo& errorCodeInfo)
{
    if (errorCodeInfo.Namespace.empty()) {
        return Format("EErrorCode::%v", errorCodeInfo.Name);
    }
    return Format("%v::EErrorCode::%v", errorCodeInfo.Namespace, errorCodeInfo.Name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
