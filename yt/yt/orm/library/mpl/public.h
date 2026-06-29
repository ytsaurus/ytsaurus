#pragma once

#include <yt/yt/core/misc/mpl.h>

#include <util/generic/fwd.h>

namespace NYT::NOrm::NMpl {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NMpl;

template <class T>
concept CString = std::same_as<TString, T> || std::same_as<std::string, T> || std::same_as<TStringBuf, T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NMpl
