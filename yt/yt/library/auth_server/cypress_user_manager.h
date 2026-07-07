#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ICypressUserManager
    : public virtual TRefCounted
{
    virtual TFuture<void> CreateUser(const std::string& name, const std::vector<std::string>& tags = {}) = 0;
    virtual TFuture<bool> CheckUserExists(const std::string& name) = 0;
    virtual TFuture<std::vector<std::string>> GetUserGroups(const std::string& name) = 0;
    virtual TFuture<void> AddUserToGroup(const std::string& name, const std::string& group) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressUserManager)

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateCypressUserManager(
    TCypressUserManagerConfigPtr config,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateNullCypressUserManager();

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateInMemoryCypressUserManager();

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateCachingCypressUserManager(
    TCachingCypressUserManagerConfigPtr config,
    ICypressUserManagerPtr userManager,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
