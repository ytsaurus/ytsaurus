// mock IKeyStoreReader, IKeyStoreWriter

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature/key_store.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TMockKeyStoreReader
    : public IKeyStoreReader
{
public:
    MOCK_METHOD(TFuture<TKeyInfoPtr>, FindKey, (const TOwnerId& ownerId, const TKeyId& keyId), (const, override));
};

DEFINE_REFCOUNTED_TYPE(TMockKeyStoreReader)

using TStrictMockKeyStoreReader = testing::StrictMock<TMockKeyStoreReader>;
DEFINE_REFCOUNTED_TYPE(TStrictMockKeyStoreReader)

////////////////////////////////////////////////////////////////////////////////

class TMockKeyStoreWriter
    : public IKeyStoreWriter
{
public:
    MOCK_METHOD(const TOwnerId&, GetOwner, (), (const, override));

    MOCK_METHOD(TFuture<void>, RegisterKey, (const TKeyInfoPtr& keyInfo), (override));
};

DEFINE_REFCOUNTED_TYPE(TMockKeyStoreWriter)

using TStrictMockKeyStoreWriter = testing::StrictMock<TMockKeyStoreWriter>;
DEFINE_REFCOUNTED_TYPE(TStrictMockKeyStoreWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
