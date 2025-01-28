#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TSignatureValidationConfig
    : public NYTree::TYsonStruct
{
    TCypressKeyReaderConfigPtr CypressKeyReader;
    TSignatureValidatorConfigPtr Validator;

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

} // namespace NYT::NSignature
