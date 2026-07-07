#pragma once

#include "public.h"

#include <yt/yt/client/misc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

class TS3ReaderConfig
    : public virtual NYTree::TYsonStruct
{
public:
    bool ValidateBlockChecksums;

    REGISTER_YSON_STRUCT(TS3ReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3ReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TS3WriterConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Minimum part size to use for multipart upload.
    //! NB: The number of parts can be limited (e.g. 10000 in AWS S3). For large
    //! chunks you might be required to increase this value.
    i64 UploadPartSize;
    //! Maximum window to be uploaded simultaneously.
    i64 UploadWindowSize;

    REGISTER_YSON_STRUCT(TS3WriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3WriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
