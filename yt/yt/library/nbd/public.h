#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/size_literals.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBlockDeviceConfigBase)
DECLARE_REFCOUNTED_STRUCT(TIdsConfig)
DECLARE_REFCOUNTED_STRUCT(TUdsConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdTestOptions)
DECLARE_REFCOUNTED_STRUCT(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Ext filesystems start at 1 KB blocks, and the kernel caps a device's logical block size at a page.
constexpr i64 MinNbdBlockSize = 1_KBs;
constexpr i64 MaxNbdBlockSize = 4_KBs;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
