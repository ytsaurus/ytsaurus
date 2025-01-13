#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
{
public:
    using TAttributeMap = THashMap<std::string, NYson::TYsonString, THash<std::string_view>, TEqualTo<std::string_view>>;
    DEFINE_BYREF_RO_PROPERTY(TAttributeMap, Attributes);
    DEFINE_BYVAL_RO_PROPERTY(i64, MasterMemoryUsage);

public:
    bool TryInsert(TStringBuf key, const NYson::TYsonString& value);
    void Set(TStringBuf key, const NYson::TYsonString& value);
    bool TryRemove(TStringBuf key);
    NYson::TYsonString Find(TStringBuf key) const;

    void Load(NCellMaster::TLoadContext& context);
    void Save(NCellMaster::TSaveContext& context) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
