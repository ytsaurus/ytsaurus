#pragma once

#include <memory>

#include <yt/yt/library/ytprof/profile.pb.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void Symbolize(NProto::Profile* profile, bool filesOnly = false);

std::pair<void*, void*> GetVdsoRange();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
