#include <yt/core/test_framework/framework.h>

#include <yp/server/lib/objects/type_info.h>

namespace NYP::NServer::NObjects::NTests {

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTypeInfo, HumanReadableName)
{
    for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
        if (type != EObjectType::Null) {
            EXPECT_FALSE(GetHumanReadableTypeName(type).empty());
            EXPECT_FALSE(GetCapitalizedHumanReadableTypeName(type).empty());
        }
    }
    EXPECT_EQ("pod set", GetHumanReadableTypeName(EObjectType::PodSet));
    EXPECT_EQ("Pod set", GetCapitalizedHumanReadableTypeName(EObjectType::PodSet));
    EXPECT_EQ("DNS record set", GetHumanReadableTypeName(EObjectType::DnsRecordSet));
    EXPECT_EQ("DNS record set", GetCapitalizedHumanReadableTypeName(EObjectType::DnsRecordSet));
    EXPECT_EQ("multi-cluster replica set", GetHumanReadableTypeName(EObjectType::MultiClusterReplicaSet));
    EXPECT_EQ("Multi-cluster replica set", GetCapitalizedHumanReadableTypeName(EObjectType::MultiClusterReplicaSet));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYP::NServer::NObjects::NTests
