#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGeneratorConfig
    : public NYTree::TYsonStruct
{
    //! Delta between signature creation and expiration.
    TDuration SignatureExpirationDelta;

    //! Margin of time synchronization error. Signature's ValidAfter is set to creation time minus this margin.
    TDuration TimeSyncMargin;

    REGISTER_YSON_STRUCT(TSignatureGeneratorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureGeneratorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TKeyRotatorConfig
    : public NYTree::TYsonStruct
{
    TDuration KeyRotationInterval;

    //! Delta between key creation and expiration.
    TDuration KeyExpirationDelta;

    //! Margin of time synchronization error. Key's ValidAfter is set to creation time minus this margin.
    TDuration TimeSyncMargin;

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

    NApi::TSerializableMasterReadOptionsPtr CypressReadOptions;

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

struct TSignatureValidationConfig
    : public NYTree::TYsonStruct
{
    TCypressKeyReaderConfigPtr CypressKeyReader;

    REGISTER_YSON_STRUCT(TSignatureValidationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureValidationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGenerationConfig
    : public NYTree::TYsonStruct
{
public:
    TCypressKeyWriterConfigPtr CypressKeyWriter;
    TSignatureGeneratorConfigPtr Generator;
    TKeyRotatorConfigPtr KeyRotator;

    REGISTER_YSON_STRUCT(TSignatureGenerationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureGenerationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureInstanceConfig
    : public NYTree::TYsonStruct
{
    TSignatureValidationConfigPtr Validation;
    TSignatureGenerationConfigPtr Generation;

    REGISTER_YSON_STRUCT(TSignatureInstanceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureInstanceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
