#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TConfig
    : public virtual NYTree::TYsonStruct
    , public TNativeSingletonsConfig
{
public:
    //! Window for an EMA counter.
    TDuration SpeedMesaurementWindow;

    NChunkClient::TMultiChunkReaderConfigPtr Reader;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_CLASS(TConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT