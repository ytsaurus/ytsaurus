#include "s3_medium.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

namespace NYT::NChunkServer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TError TS3Medium::TryUpdateConfig(
    NChunkClient::TS3MediumConfigPtr newConfig,
    const NSecurityServer::ISecurityManagerPtr& securityManager)
{
    auto checkRestrictedField = [&] (const auto& fieldName, const auto& oldValue, const auto& newValue) {
        if (oldValue == newValue) {
            return;
        }

        // Allow only superusers to change these fields.
        NSecurityServer::ValidateSuperuserOnAttributeModification(
            securityManager,
            Format("Offshore medium's %v", fieldName));
    };

    try {
        // TODO(pavel-bash): next step would be to allow changing these fields if the
        // medium is empty, i.e. there are no objects referencing it.
        checkRestrictedField("url", Config_->Url, newConfig->Url);
        checkRestrictedField("region", Config_->Region, newConfig->Region);
        checkRestrictedField("bucket", Config_->Bucket, newConfig->Bucket);
        checkRestrictedField("prefix", Config_->Prefix, newConfig->Prefix);
    } catch (const TErrorException& ex) {
        return TError(ex);
    }

    Config_ = std::move(newConfig);
    return TError();
}

bool TS3Medium::IsDomestic() const
{
    return false;
}

std::string TS3Medium::GetLowercaseObjectName() const
{
    return "S3 medium";
}

std::string TS3Medium::GetCapitalizedObjectName() const
{
    return "S3 medium";
}

std::string TS3Medium::GetMediumType() const
{
    return "s3";
}

void TS3Medium::FillMediumDescriptor(NChunkClient::NProto::TMediumDirectory::TMediumDescriptor* protoItem) const
{
    TMedium::FillMediumDescriptor(protoItem);

    // TODO(achulkov2): [PLater/Review] What is the best way to avoid copy-paste with medium_directory.cpp?
    ToProto(protoItem->mutable_s3_medium_descriptor()->mutable_config(), ConvertToYsonString(Config_).ToString());
}

void TS3Medium::Save(NCellMaster::TSaveContext& context) const
{
    TMedium::Save(context);

    using NYT::Save;

    Save(context, *Config_);
}

void TS3Medium::Load(NCellMaster::TLoadContext& context)
{
    TMedium::Load(context);

    using NYT::Load;

    Load(context, *Config_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
