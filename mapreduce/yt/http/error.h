#pragma once

// NOTE: this file is for backward compatibility
// TODO: fix including files and remove this header

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/node.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NYT {
    using TError = TYtError;
} // namespace NYT
