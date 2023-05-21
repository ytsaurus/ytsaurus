#pragma once

#include "private.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EServerHandshakeFlags, ui16,
    ((NBD_FLAG_FIXED_NEWSTYLE)(1))
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EClientHandshakeFlags, ui32,
    ((NBD_FLAG_C_FIXED_NEWSTYLE)(1))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EClientOption, ui32,
    ((NBD_OPT_EXPORT_NAME)(1))
    ((NBD_OPT_ABORT)(2))
    ((NBD_OPT_LIST)(3))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EServerOptionReply, ui32,
    ((NBD_REP_ACK)(1))
    ((NBD_REP_SERVER)(2))
    ((NBD_REP_ERR_UNSUP)((1ULL << 31) + 1))
    ((NBD_REP_ERR_INVALID)((1ULL << 31) + 3))
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(ECommandFlags, ui16,
    ((NBD_CMD_FLAG_FUA)(1))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECommandType, ui16,
    ((NBD_CMD_READ)(0))
    ((NBD_CMD_WRITE)(1))
    ((NBD_CMD_DISC)(2))
    ((NBD_CMD_FLUSH)(3))
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(ETransmissionFlags, ui16,
    ((NBD_FLAG_HAS_FLAGS)(1))
    ((NBD_FLAG_READ_ONLY)(2))
    ((NBD_FLAG_SEND_FLUSH)(4))
    ((NBD_FLAG_SEND_FUA)(8))
);

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EServerError, ui32,
    ((NBD_OK)(0))
    ((NBD_EPERM)(1))
    ((NBD_EIO)(5))
    ((NBD_EINVAL)(22))
    ((NBD_ENOSPC)(28))
    ((NBD_ENOTSUP)(95))
);

#pragma pack(push, 1)

struct TServerHandshakeMessage
{
    ui64 Magic1;
    static constexpr ui64 ExpectedHostMagic1 = 0x4e42444d41474943; // NBDMAGIC

    ui64 Magic2;
    static constexpr ui64 ExpectedHostMagic2 = 0x49484156454F5054; // IHAVEOPT

    EServerHandshakeFlags Flags;
};

struct TClientOptionMessage
{
    ui64 Magic;
    static constexpr ui64 ExpectedHostMagic = 0x49484156454F5054; // IHAVEOPT

    EClientOption Option;

    ui32 Length;
    static constexpr size_t MaxLength = 1_KB;
};

struct TServerOptionMessage
{
    ui64 Magic;
    static constexpr ui64 ExpectedHostMagic = 0x3e889045565a9;

    EClientOption Option;
    EServerOptionReply Reply;
    ui32 Length;
};

struct TServerExportNameMessage
{
    ui64 Size;
    ETransmissionFlags Flags;
    std::array<char, 124> Reserved;
};

struct TClientRequestMessage
{
    ui32 Magic;
    static constexpr ui32 ExpectedHostMagic = 0x25609513;

    ECommandFlags Flags;
    ECommandType Type;
    ui64 Cookie;
    ui64 Offset;

    static constexpr ui32 MaxLength = 32_MB;
    ui32 Length;
};

struct TServerResponseMessage
{
    ui32 Magic;
    static constexpr ui32 ExpectedHostMagic = 0x67446698;

    EServerError Error;
    ui64 Cookie;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
