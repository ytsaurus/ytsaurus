#include "state.h"

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ValidateStateName(const std::string& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("State name is empty")
            << TErrorAttribute("name", name);
    }
    if (name == "/") {
        THROW_ERROR_EXCEPTION("State name is root")
            << TErrorAttribute("name", name);
    }
    if (name.back() == '/') {
        THROW_ERROR_EXCEPTION("State name ends with '/'")
            << TErrorAttribute("name", name);
    }
    if (name.front() != '/') {
        THROW_ERROR_EXCEPTION("State name does not start with '/'")
            << TErrorAttribute("name", name);
    }
    if (auto position = name.find("//"); position != std::string::npos) {
        THROW_ERROR_EXCEPTION("State name contains two adjacent '/'")
            << TErrorAttribute("name", name)
            << TErrorAttribute("position", position);
    }
}

std::string ExtendStateNamePrefix(TStringBuf prefix, TStringBuf segment)
{
    std::string result;
    result.reserve(prefix.size() + segment.size() + 1);
    result.append(prefix.data(), prefix.size());
    if (!segment.StartsWith('/')) {
        result.push_back('/');
    }
    result.append(segment.data(), segment.size());
    ValidateStateName(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(NYson::IYsonConsumer* consumer, const TNullState& /*state*/)
{
    consumer->OnEntity();
}

void Deserialize(TNullState& /*state*/, NYson::TYsonPullParserCursor* cursor)
{
    cursor->SkipComplexValue();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
