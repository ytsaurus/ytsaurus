#pragma once

#include <cstdlib>
#include <string>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! RAII helper to set an environment variable and restore (or unset) it on exit.
class TEnvGuard
{
public:
    TEnvGuard(const std::string& name, const std::string& value)
        : TEnvGuard(name)
    {
        ::setenv(name.c_str(), value.c_str(), /*overwrite*/ 1);
    }

    //! Unsets the variable for the lifetime of the guard.
    explicit TEnvGuard(const std::string& name)
        : Name_(name)
    {
        const char* old = std::getenv(name.c_str());
        if (old) {
            OldValue_ = old;
            HadOldValue_ = true;
        }
        ::unsetenv(name.c_str());
    }

    ~TEnvGuard()
    {
        if (HadOldValue_) {
            ::setenv(Name_.c_str(), OldValue_.c_str(), /*overwrite*/ 1);
        } else {
            ::unsetenv(Name_.c_str());
        }
    }

    TEnvGuard(const TEnvGuard&) = delete;
    TEnvGuard& operator=(const TEnvGuard&) = delete;

private:
    std::string Name_;
    std::string OldValue_;
    bool HadOldValue_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
