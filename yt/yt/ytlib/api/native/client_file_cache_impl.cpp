#include "client_impl.h"
#include "connection.h"
#include "transaction.h"

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int FileCacheHashDigitCount = 2;

TYPath GetFilePathInCache(const TString& md5, const TYPath& cachePath)
{
    auto lastDigits = md5.substr(md5.size() - FileCacheHashDigitCount);
    return cachePath + "/" + lastDigits + "/" + md5;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TClient::SetTouchedAttribute(
    const TString& destination,
    const TPrerequisiteOptions& options,
    TTransactionId transactionId)
{
    auto fileCacheClient = Connection_->CreateNativeClient(TClientOptions(NSecurityClient::FileCacheUserName));

    // Set /@touched attribute.
    {
        auto setNodeOptions = TSetNodeOptions();
        setNodeOptions.PrerequisiteTransactionIds = options.PrerequisiteTransactionIds;
        setNodeOptions.PrerequisiteRevisions = options.PrerequisiteRevisions;
        setNodeOptions.TransactionId = transactionId;

        auto asyncResult = fileCacheClient->SetNode(destination + "/@touched", ConvertToYsonString(true), setNodeOptions);
        auto rspOrError = WaitFor(asyncResult);

        if (rspOrError.GetCode() != NCypressClient::EErrorCode::ConcurrentTransactionLockConflict) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error setting /@touched attribute");
        }

        YT_LOG_DEBUG(
            "Attribute /@touched set (Destination: %v)",
            destination);
    }
}

TGetFileFromCacheResult TClient::DoGetFileFromCache(
    const TString& md5,
    const TGetFileFromCacheOptions& options)
{
    TGetFileFromCacheResult result;
    auto destination = GetFilePathInCache(md5, options.CachePath);

    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto req = TYPathProxy::Get(destination + "/@");
    NCypressClient::SetTransactionId(req, options.TransactionId);
    
    ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
        "md5"
    });

    auto rspOrError = WaitFor(proxy->Execute(req));
    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(
            rspOrError,
            "File is missing "
            "(Destination: %v, MD5: %v)",
            destination,
            md5);

        return result;
    }

    auto rsp = rspOrError.Value();
    auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

    auto originalMD5 = attributes->Get<TString>("md5", TString());
    if (md5 != originalMD5) {
        YT_LOG_DEBUG(
            "File has incorrect MD5 hash "
            "(Destination: %v, ExpectedMD5: %v, OriginalMD5: %v)",
            destination,
            md5,
            originalMD5);

        return result;
    }

    try {
        SetTouchedAttribute(destination, TPrerequisiteOptions(), options.TransactionId);
    } catch (const NYT::TErrorException& ex) {
        YT_LOG_DEBUG(
            ex.Error(),
            "Failed to set touched attribute on file (Destination: %v)",
            destination);
        return result;
    }

    result.Path = destination;
    return result;
}

TPutFileToCacheResult TClient::DoAttemptPutFileToCache(
    const TYPath& path,
    const TString& expectedMD5,
    const TPutFileToCacheOptions& options,
    NLogging::TLogger logger)
{
    auto Logger = logger;

    TPutFileToCacheResult result;

    // Start transaction.
    NApi::ITransactionPtr transaction;
    {
        auto transactionStartOptions = TTransactionStartOptions();
        transactionStartOptions.ParentId = options.TransactionId;

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Putting file %v to cache", path));
        transactionStartOptions.Attributes = std::move(attributes);

        auto asyncTransaction = StartTransaction(ETransactionType::Master, transactionStartOptions);
        transaction = WaitFor(asyncTransaction)
            .ValueOrThrow();

        YT_LOG_DEBUG(
            "Transaction started (TransactionId: %v)",
            transaction->GetId());
    }

    Logger.AddTag("TransactionId: %v", transaction->GetId());

    // Acquire lock.
    TYPath objectIdPath;
    {
        TLockNodeOptions lockNodeOptions;
        lockNodeOptions.TransactionId = transaction->GetId();
        auto lockResult = DoLockNode(path, ELockMode::Exclusive, lockNodeOptions);
        objectIdPath = FromObjectId(lockResult.NodeId);

        YT_LOG_DEBUG(
            "Lock for node acquired (LockId: %v)",
            lockResult.LockId);
    }

    // Check permissions.
    {
        TCheckPermissionOptions checkPermissionOptions;
        checkPermissionOptions.TransactionId = transaction->GetId();

        InternalValidatePermission(objectIdPath, EPermission::Read, checkPermissionOptions);
        InternalValidatePermission(objectIdPath, EPermission::Remove, checkPermissionOptions);

        auto usePermissionResult = InternalCheckPermission(options.CachePath, EPermission::Use, checkPermissionOptions);
        auto writePermissionResult = InternalCheckPermission(options.CachePath, EPermission::Write, checkPermissionOptions);
        if (usePermissionResult.Action == ESecurityAction::Deny && writePermissionResult.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("You need %Qlv or %Qlv permission to use file cache",
                EPermission::Use,
                EPermission::Write)
                << usePermissionResult.ToError(Options_.GetUser(), EPermission::Use)
                << writePermissionResult.ToError(Options_.GetUser(), EPermission::Write);
        }
    }

    // Check that MD5 hash is equal to the original MD5 hash of the file.
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto req = TYPathProxy::Get(objectIdPath + "/@");

        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "md5"
        });

        NCypressClient::SetTransactionId(req, transaction->GetId());

        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting MD5 hash of file %v",
            path);

        auto rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        auto md5 = attributes->Get<TString>("md5");
        if (expectedMD5 != md5) {
            THROW_ERROR_EXCEPTION(
                "MD5 mismatch: expected %v, got %v",
                expectedMD5,
                md5);
        }

        YT_LOG_DEBUG(
            "MD5 hash checked (MD5: %v)",
            expectedMD5);
    }

    auto destination = GetFilePathInCache(expectedMD5, options.CachePath);
    auto fileCacheClient = Connection_->CreateNativeClient(TClientOptions(NSecurityClient::FileCacheUserName));

    // Copy file.
    {
        auto copyOptions = TCopyNodeOptions();
        copyOptions.TransactionId = transaction->GetId();
        copyOptions.Recursive = true;
        copyOptions.IgnoreExisting = true;
        copyOptions.PrerequisiteRevisions = options.PrerequisiteRevisions;
        copyOptions.PrerequisiteTransactionIds = options.PrerequisiteTransactionIds;

        WaitFor(fileCacheClient->CopyNode(objectIdPath, destination, copyOptions))
            .ThrowOnError();

        YT_LOG_DEBUG(
            "File has been copied to cache (Destination: %v)",
            destination);
    }

    SetTouchedAttribute(destination, options, transaction->GetId());

    WaitFor(transaction->Commit())
        .ThrowOnError();

    result.Path = destination;
    return result;
}

TPutFileToCacheResult TClient::DoPutFileToCache(
    const TYPath& path,
    const TString& expectedMD5,
    const TPutFileToCacheOptions& options)
{
    NLogging::TLogger logger = Logger;
    auto Logger = logger
        .AddTag("Path: %v", path)
        .AddTag("Command: PutFileToCache");

    int retryAttempts = 0;
    while (true) {
        try {
            return DoAttemptPutFileToCache(path, expectedMD5, options, logger);
        } catch (const TErrorException& ex) {
            auto error = ex.Error();
            ++retryAttempts;
            if (retryAttempts < options.RetryCount && error.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
                YT_LOG_DEBUG(error, "Put file to cache failed, doing another attempt");
            } else {
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
