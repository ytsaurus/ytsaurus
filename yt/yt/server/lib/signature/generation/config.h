#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/config.h>

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
    //! Options of key rotation executor (period, etc.).
    NConcurrency::TRetryingPeriodicExecutorOptions KeyRotationOptions;

    //! Delta between key creation and expiration.
    TDuration KeyExpirationDelta;

    //! Margin of time synchronization error. Key's ValidAfter is set to creation time minus this margin.
    TDuration TimeSyncMargin;

    REGISTER_YSON_STRUCT(TKeyRotatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyRotatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyWriterConfig
    : public NYTree::TYsonStruct
{
    //! Prefix path for public keys (will be stored as <Path>/<Owner>/<KeyId>).
    NYPath::TYPath Path;

    //! Time to wait after expiration before deleting keys from Cypress.
    TDuration KeyDeletionDelay;

    //! Maximum key count allowed.
    std::optional<int> MaxKeyCount;

    REGISTER_YSON_STRUCT(TCypressKeyWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGenerationConfig
    : public NYTree::TYsonStruct
{
    TCypressKeyWriterConfigPtr CypressKeyWriter;
    TSignatureGeneratorConfigPtr Generator;
    TKeyRotatorConfigPtr KeyRotator;

    REGISTER_YSON_STRUCT(TSignatureGenerationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignatureGenerationConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
