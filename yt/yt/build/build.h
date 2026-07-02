#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

int GetVersionMajor();
int GetVersionMinor();
int GetVersionPatch();
const char* GetBranch();
const char* GetVersion();
//! Returns the Query Tracker component version (separate from the global YT version).
//! Pass to SetVersionProvider() so that GetVersion(), orchid /service/version, and
//! the get_version RPC command all report the QT version for ytserver-query-tracker
//! and ytserver-yql-agent.
const char* GetQueryTrackerVersion();
const char* GetVersionType();
const char* GetBuildHost();
const char* GetBuildTime();

using TVersionProvider = const char*(*)();

//! Overrides the version string returned by GetVersion().
//! Must be called before any service code starts (e.g., at the beginning of DoRun/DoStart).
void SetVersionProvider(TVersionProvider provider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

