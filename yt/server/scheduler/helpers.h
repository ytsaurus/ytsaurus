#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <core/ytree/public.h>

#include <ytlib/object_client/public.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/hive/cluster_directory.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue);
Stroka TrimCommandForBriefSpec(const Stroka& command);

////////////////////////////////////////////////////////////////////////////////

//class TMultiCellBatchResponse
//{
//public:
//    TMultiCellBatchResponse(
//        const std::vector<NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr>& batchResponses,
//        const std::vector<std::pair<int, int>>& index,
//        const std::multimap<Stroka, int>& keyToIndexes);
//
//    int GetSize() const;
//
//    TError GetCumulativeError() const;
//
//    template <class TTypedResponse>
//    TIntrusivePtr<TTypedResponse> GetResponse(int index) const;
//    NYTree::TYPathResponsePtr GetResponse(int index) const;
//
//    template <class TTypedResponse>
//    TIntrusivePtr<TTypedResponse> FindResponse(const Stroka& key) const;
//    NYTree::TYPathResponsePtr FindResponse(const Stroka& key) const;
//
//    template <class TTypedResponse>
//    TIntrusivePtr<TTypedResponse> GetResponse(const Stroka& key) const;
//    NYTree::TYPathResponsePtr GetResponse(const Stroka& key) const;
//
//    template <class TTypedResponse>
//    std::vector< TIntrusivePtr<TTypedResponse> > GetResponses(const Stroka& key = "") const;
//    template <class TTypedResponse>
//    TNullable<std::vector<TIntrusivePtr<TTypedResponse>>> FindResponses(const Stroka& key = "") const;
//    std::vector<NYTree::TYPathResponsePtr> GetResponses(const Stroka& key = "") const;
//    TNullable<std::vector<NYTree::TYPathResponsePtr>> FindResponses(const Stroka& key = "") const;
//
//    bool IsOK() const;
//    operator TError() const;
//
//private:
//    std::vector<NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> BatchResponses_;
//
//    // Index of batch + number of request inside batch.
//    std::vector<std::pair<int, int>> ResponseIndex_;
//
//    std::multimap<Stroka, int> KeyToIndexes_;
//};
//
//////////////////////////////////////////////////////////////////////
//
//class TMultiCellBatchRequest
//{
//public:
//    TMultiCellBatchRequest(NCellDirectory::TCellDirectoryPtr cellDirectory, bool throwIfCellIsMissing);
//
//    bool AddRequest(NYTree::TYPathRequestPtr req, const Stroka& key, NObjectClient::TCellId cellId);
//    bool AddRequestForTransaction(NYTree::TYPathRequestPtr req, const Stroka& key, const NObjectClient::TTransactionId& id);
//
//    TMultiCellBatchResponse Execute(IInvokerPtr invoker = GetCurrentInvoker());
//
//private:
//    bool Init(NObjectClient::TCellId cellId);
//
//    std::map<NObjectClient::TCellId, NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr> BatchRequests_;
//    std::vector<std::pair<NObjectClient::TCellId, int>> RequestIndex_;
//    NCellDirectory::TCellDirectoryPtr CellDirectory_;
//    bool ThrowIfCellIsMissing_;
//
//    std::multimap<Stroka, int> KeyToIndexes_;
//};

////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
