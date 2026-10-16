#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

#include <string>
#include <string_view>
#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFlowCoreBuildInfo)

//! Source-control and build-environment metadata baked into the binary at
//! build time.
//!
//! VCS fields work uniformly across arc, git: `ya-bin dump vcs-info`
//! normalizes them into the same set of fields regardless of the
//! underlying VCS.
//!
//! All string fields are empty (and numeric fields are at their sentinel
//! values, see below) when the corresponding piece of build info was
//! unavailable at build time.
struct TFlowCoreBuildInfo
    : public NYTree::TYsonStruct
{
    // ---- VCS ----

    std::string CommitHash;
    std::string Author;
    std::string Branch;
    std::string Tag;
    //! First-line commit summary, extracted from the "Summary:" entry of
    //! the SCM_DATA macro. May be empty.
    std::string CommitSummary;
    //! Monotonic trunk revision number (e.g. 123456 rendered as "r123456").
    //! Populated only for SVN-bridge trunk builds; -1 when absent.
    //! Mirrors the sentinel used by build/scripts/vcs_info.py.
    int SvnRevision = -1;

    // ---- Build environment ----

    //! Hostname of the machine that built this binary.
    std::string BuildHost;
    //! Unix timestamp (seconds since epoch) of the build.
    i64 BuildTimestamp = 0;

    //! True if every field is at its absent/sentinel value.
    bool IsEmpty() const;

    REGISTER_YSON_STRUCT(TFlowCoreBuildInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowCoreBuildInfo)

//! Returns the build metadata for this binary.
//! The same instance is returned on every call.
const TFlowCoreBuildInfoPtr& GetFlowCoreBuildInfo();

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Reads the value of a `<Key>:` field from SCM_DATA-formatted text.
//! Fields are one per line, optionally indented. Returns an empty
//! string if the field is missing.
std::string ExtractScmField(std::string_view scmData, std::string_view key);

//! Returns the prefix of |text| up to (excluding) the first non-ASCII symbol,
//! together with a flag indicating whether such a byte was encountered.
//!
//! Used to render the commit summary safely: the YT HTTP proxy
//! mojibakes any non-ASCII byte.
std::pair<std::string_view, bool> ExtractAsciiPrefix(std::string_view text);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
