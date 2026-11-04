#pragma once

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/phoenix/type_decl.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! The journal NBD device as the agent resolved it from the operation options and the disk request.
struct TJournalNbdSpec
{
    //! YSON-serialized NNbd::NJournal::TJournalBlockDeviceConfig.
    NYson::TYsonString Config;

    //! The requested disk space, truncated to a whole number of blocks.
    i64 DeviceSize = 0;
    i64 BlockSize = 0;

    std::string Account;

    i64 MemoryUsage = 0;

    PHOENIX_DECLARE_TYPE(TJournalNbdSpec, 0x7c1a9f42);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
