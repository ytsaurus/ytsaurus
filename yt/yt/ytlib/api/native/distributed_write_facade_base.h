#pragma once

#include "client_impl.h"

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CDistributedWriteFacadeTraits = requires
{
    typename T::TStartOptions;
    typename T::TPingOptions;
    typename T::TFinishOptions;

    typename T::TSession;
    typename T::TWriteFragmentResult;

    typename T::TSessionWithCookies;
    typename T::TSessionWithResults;

    typename T::TSignedSessionPtr;
    typename T::TSignedCookiePtr;

    requires std::is_same_v<decltype(T::ObjectType), const NObjectClient::EObjectType>;
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
class TDistributedWriteStartFacadeBase
{
public:
    TDistributedWriteStartFacadeBase(
        const IClientPtr& client,
        const NLogging::TLogger& logger);

    TTraits::TSessionWithCookies StartSession(
        const NYPath::TRichYPath& richPath,
        const TTraits::TStartOptions& options);

protected:
    NApi::ITransactionPtr StartMasterTransaction(const TTraits::TStartOptions& options);

    NChunkClient::TUserObject RequestUserObject(
        const NYPath::TYPath& path,
        NApi::ITransactionPtr transaction);

    TDerived* ToDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* ToDerived() const
    {
        return static_cast<const TDerived*>(this);
    }

protected:
    // Must be defined by derivatives
    NYTree::INodePtr RequestExtendedObjectAttributes(
        const NYPath::TRichYPath& path,
        NObjectClient::TCellTag externalCellTag,
        const NYPath::TYPath& objectIdPath,
        const NChunkClient::TUserObject& userObject) = delete;

    void PreUploadActions() = delete;

    std::tuple<NTableClient::TMasterTableSchemaId, NCypressClient::TTransactionId> BeginUpload(
        const NYPath::TRichYPath& path,
        NObjectClient::TCellTag nativeCellTag,
        const NYPath::TYPath& objectIdPath,
        NCypressClient::TTransactionId transactionId) = delete;

    NChunkClient::TChunkListId RequestUploadParameters(
        const NYPath::TRichYPath& path,
        NObjectClient::TCellTag externalCellTag,
        const NYPath::TYPath& objectIdPath,
        NCypressClient::TTransactionId uploadTransactionId) = delete;

    TTraits::TSession CreateSession(
        NCypressClient::TTransactionId masterTransactionId,
        NCypressClient::TTransactionId uploadTransactionId,
        NChunkClient::TChunkListId rootChunkListId,
        const NYPath::TRichYPath& path,
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag externalCellTag,
        NTransactionClient::TTimestamp timestamp,
        NYTree::INodePtr attributes) = delete;

private:
    const IClientPtr Client_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
class TDistributedWritePingFacadeBase
{
public:
    explicit TDistributedWritePingFacadeBase(const IClientPtr& client);

    void PingSession(
        TTraits::TSignedSessionPtr session,
        const TTraits::TPingOptions& options);

protected:
    // Must be defined by derivatives
    NCypressClient::TTransactionId GetMasterTransaction(const TTraits::TSession& session) = delete;

private:
    const IClientPtr Client_;

    TDerived* ToDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* ToDerived() const
    {
        return static_cast<const TDerived*>(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, CDistributedWriteFacadeTraits TTraits>
class TDistributedWriteFinishFacadeBase
{
public:
    TDistributedWriteFinishFacadeBase(
        const IClientPtr& client,
        const NLogging::TLogger& logger);

    void FinishSession(
        const TTraits::TSessionWithResults& sessionWithResults,
        const TTraits::TFinishOptions& options);

protected:
    TDerived* ToDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* ToDerived() const
    {
        return static_cast<const TDerived*>(this);
    }

protected:
    // Must be defined by derivatives
    NCypressClient::TTransactionId GetMasterTransaction(const TTraits::TSession& session) = delete;

    NObjectClient::TObjectId GetObjectId(const TTraits::TSession& session) = delete;

    NYPath::TRichYPath GetPath(const TTraits::TSession& session) = delete;

    NObjectClient::TCellTag GetExternalCellTag(const TTraits::TSession& session) = delete;

    void SortResults(
        TNonNullPtr<std::vector<typename TTraits::TWriteFragmentResult>> results,
        const TTraits::TSession& session) = delete;

    void EndUpload(
        const TTraits::TSession& session,
        const NChunkClient::NProto::TDataStatistics& dataStatistics) = delete;

private:
    const IClientPtr Client_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

#define DISTRIBUTED_WRITE_FACADE_INL_H
#include "distributed_write_facade_base-inl.h"
#undef DISTRIBUTED_WRITE_FACADE_INL_H
