#include "ephemeral_node.h"

#include "attributes_helpers.h"
#include "backoff.h"
#include "private.h"

#include <yt/core/ytree/convert.h>
#include <yt/ytlib/object_client/public.h>

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>

#include <util/generic/guid.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsNodeNotFound(const TError& error)
{
   return error.GetCode() == NYTree::EErrorCode::ResolveError;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TEphemeralNodeKeeper);

class TEphemeralNodeKeeper
    : public TRefCounted
{
private:
    TString DirectoryPath_;
    TString Name_;
    THashMap<TString, TString> Attributes_;
    TDuration SessionTimeout_;

    IClientPtr Client_;

    TString Path_;

    TBackoff Backoff_;

    NLogging::TLogger Logger;

public:
    TEphemeralNodeKeeper(
        TString directoryPath,
        TString name,
        THashMap<TString, TString> attributes,
        TDuration sessionTimeout,
        IClientPtr client)
        : DirectoryPath_(std::move(directoryPath))
        , Name_(std::move(name))
        , Attributes_(std::move(attributes))
        , SessionTimeout_(sessionTimeout)
        , Client_(std::move(client))
        , Path_(TString::Join(DirectoryPath_, '/', Name_))
        , Logger(NLogging::TLogger(ServerLogger).AddTag("Path: %v", Path_))
    {
        CreateNode();
    }

private:
    void CreateNode()
    {
        auto result = TryCreateNode(Path_);
        if (result.IsOK()) {
            Backoff_.Reset();
            LOG_DEBUG("Ephemeral node created");
            TouchNodeLater(GetNextTouchDelay());
        } else {
            LOG_WARNING("Cannot create ephemeral node, scheduling retry");
            CreateNodeLater(Backoff_.GetNextPause());
        }
    }

    TErrorOr<void> TryCreateNode(const TString& nodePath)
    {
        auto startTxResult = WaitFor(Client_->StartTransaction(
            NTransactionClient::ETransactionType::Master));

        if (!startTxResult.IsOK()) {
            return startTxResult;
        }

        auto txClient = startTxResult.ValueOrThrow();

        TCreateNodeOptions createOptions;
        createOptions.Recursive = false;
        createOptions.IgnoreExisting = false;

        auto nodeAttributes = ConvertToAttributes(Attributes_);
        nodeAttributes->Set("expiration_time", GetExpirationTimeFromNow());
        createOptions.Attributes = std::move(nodeAttributes);

        auto createResult = WaitFor(txClient->CreateNode(
            nodePath,
            NObjectClient::EObjectType::StringNode,
            createOptions));

        if (!createResult.IsOK()) {
            return createResult;
        }

        return WaitFor(txClient->Commit());
    }

    void CreateNodeLater(const TDuration& delay)
    {
        TDelayedExecutor::Submit(
            BIND(&TEphemeralNodeKeeper::CreateNode, MakeWeak(this)),
            delay);
    }

    void TouchNode()
    {
        auto result = TryTouchNode();
        if (result.IsOK()) {
            Backoff_.Reset();
            LOG_DEBUG("Ephemeral node touched");
            TouchNodeLater(GetNextTouchDelay());
        } else if (IsNodeNotFound(result)) {
            LOG_WARNING("Ephemeral node lost, recreating it");
            CreateNode();
        } else {
            LOG_WARNING(result, "Error while touching node");
            TouchNodeLater(Backoff_.GetNextPause());
        }
    }

    TErrorOr<void> TryTouchNode()
    {
        return WaitFor(Client_->SetNode(
            TString::Join(Path_, "/@expiration_time"),
            ConvertToYsonString(GetExpirationTimeFromNow())));
    }

    void TouchNodeLater(const TDuration& delay)
    {
        TDelayedExecutor::Submit(
            BIND(&TEphemeralNodeKeeper::TouchNode, MakeWeak(this)),
            delay);
    }

    TInstant GetExpirationTimeFromNow() const
    {
        return Now() + SessionTimeout_;
    }

    TDuration GetNextTouchDelay() const
    {
        return AddJitter(SessionTimeout_ * 0.5, /*jitter=*/ 0.2);
    }
};

DEFINE_REFCOUNTED_TYPE(TEphemeralNodeKeeper);

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeKeeperHolder
    : public NInterop::IEphemeralNodeKeeper
{
public:
    TEphemeralNodeKeeperHolder(
        TEphemeralNodeKeeperPtr nodeKeeper)
        : NodeKeeper(std::move(nodeKeeper))
    {}

    void Release() override
    {
        NodeKeeper.Reset();
    }

private:
    TEphemeralNodeKeeperPtr NodeKeeper;
};

////////////////////////////////////////////////////////////////////////////////

NInterop::IEphemeralNodeKeeperPtr CreateEphemeralNodeKeeper(
    NApi::IClientPtr client,
    TString directoryPath,
    TString name,
    THashMap<TString, TString> attributes,
    TDuration sessionTimeout)
{
    auto nodeKeeper = New<TEphemeralNodeKeeper>(
        std::move(directoryPath),
        std::move(name),
        std::move(attributes),
        sessionTimeout,
        std::move(client));

    return std::make_unique<TEphemeralNodeKeeperHolder>(
       std::move(nodeKeeper));
}

}   // namespace NClickHouse
}   // namespace NYT
