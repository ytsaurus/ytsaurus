#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature/components.h>
#include <yt/yt/server/lib/signature/config.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NYson;

using testing::_;
using testing::Return;

using TStrictMockClient = testing::StrictMock<TMockClient>;
DEFINE_REFCOUNTED_TYPE(TStrictMockClient)

////////////////////////////////////////////////////////////////////////////////

struct TSignatureComponentsTest
    : public ::testing::Test
{
    TCypressKeyReaderConfigPtr CypressKeyReaderConfig = New<TCypressKeyReaderConfig>();
    TCypressKeyWriterConfigPtr CypressKeyWriterConfig = New<TCypressKeyWriterConfig>();
    TKeyRotatorConfigPtr KeyRotatorConfig = New<TKeyRotatorConfig>();
    TSignatureGeneratorConfigPtr GeneratorConfig = New<TSignatureGeneratorConfig>();
    TSignatureGenerationConfigPtr GenerationConfig = New<TSignatureGenerationConfig>();
    TSignatureValidationConfigPtr ValidationConfig = New<TSignatureValidationConfig>();
    TSignatureComponentsConfigPtr Config = New<TSignatureComponentsConfig>();
    TIntrusivePtr<TStrictMockClient> Client = New<TStrictMockClient>();
    TSignatureComponentsPtr Instance;

    TSignatureComponentsTest()
    {
        CypressKeyWriterConfig->OwnerId = TOwnerId("test");
        GenerationConfig->CypressKeyWriter = CypressKeyWriterConfig;
        GenerationConfig->KeyRotator = KeyRotatorConfig;
        GenerationConfig->Generator = GeneratorConfig;
        ValidationConfig->CypressKeyReader = CypressKeyReaderConfig;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureComponentsTest, EmptyInit)
{
    Instance = New<TSignatureComponents>(Config, Client, GetCurrentInvoker());
    EXPECT_TRUE(Instance->Initialize().IsSet());
    EXPECT_TRUE(Instance->StartRotation().IsSet());
    EXPECT_TRUE(Instance->StopRotation().IsSet());

    auto generator = Instance->GetSignatureGenerator();
    auto validator = Instance->GetSignatureValidator();

    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(generator->Sign("test")), "unsupported");
    auto signature = New<TSignature>();
    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(validator->Validate(signature)), "unsupported");
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureComponentsTest, Generation)
{
    EXPECT_CALL(*Client, CreateNode(_, _, _))
        .Times(2)
        .WillRepeatedly(Return(MakeFuture(TNodeId())));
    EXPECT_CALL(*Client, SetNode(_, _, _))
        .WillOnce(Return(VoidFuture));

    Config->Generation = GenerationConfig;
    Instance = New<TSignatureComponents>(Config, Client, GetCurrentInvoker());
    WaitFor(Instance->Initialize())
        .ThrowOnError();

    WaitFor(Instance->StartRotation())
        .ThrowOnError();

    WaitFor(Instance->StopRotation())
        .ThrowOnError();

    auto generator = Instance->GetSignatureGenerator();
    EXPECT_TRUE(generator);
    auto signature = generator->Sign("test");
    EXPECT_EQ(signature->Payload(), "test");
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureComponentsTest, Validation)
{
    Config->Validation = ValidationConfig;
    Instance = New<TSignatureComponents>(Config, Client, GetCurrentInvoker());
    WaitFor(Instance->Initialize())
        .ThrowOnError();

    auto validator = Instance->GetSignatureValidator();
    auto signature = New<TSignature>();
    auto validationResult = WaitFor(validator->Validate(signature))
        .ValueOrThrow();
    EXPECT_FALSE(validationResult);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
