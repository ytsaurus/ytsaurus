#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGeneratorConfig
    : public NYTree::TYsonStruct
{
    //! Delta between key creation and expiration.
    TDuration KeyExpirationDelta;

    //! Delta between signature creation and expiration.
    TDuration SignatureExpirationDelta;

    //! Margin of time synchronization error. ValidAfter is set to creation time minus this margin.
    TDuration TimeSyncMargin;

    REGISTER_YSON_STRUCT(TSignatureGeneratorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureGeneratorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureValidatorConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TSignatureValidatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TKeyRotatorConfig
    : public NYTree::TYsonStruct
{
    TDuration KeyRotationInterval;

    REGISTER_YSON_STRUCT(TKeyRotatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyRotatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyReaderConfig
    : public NYTree::TYsonStruct
{
    //! Prefix path for public keys (will be read from <Path>/<OwnerId>/<KeyId>).
    NYPath::TYPath Path;

    REGISTER_YSON_STRUCT(TCypressKeyReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyWriterConfig
    : public NYTree::TYsonStruct
{
    //! Prefix path for public keys (will be stored as <Path>/<Owner>/<KeyId>).
    NYPath::TYPath Path;

    TOwnerId OwnerId;

    //! Time to wait after expiration before deleting keys from Cypress.
    TDuration KeyDeletionDelay;

    //! Maximum key count allowed.
    std::optional<int> MaxKeyCount;

    REGISTER_YSON_STRUCT(TCypressKeyWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
