#pragma once

#include "http.h"

#include <mapreduce/yt/interface/common.h>

#include <util/generic/maybe.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TTransactionId StartTransaction(
    const Stroka& serverName,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout = Nothing(),
    bool pingAncestors = false,
    const TMaybe<TNode>& attributes = Nothing());

void PingTransaction(
    const Stroka& serverName,
    const TTransactionId& transactionId);

void AbortTransaction(
    const Stroka& serverName,
    const TTransactionId& transactionId);

void CommitTransaction(
    const Stroka& serverName,
    const TTransactionId& transactionId);

////////////////////////////////////////////////////////////////////////////////

Stroka Get(const Stroka& serverName, const TYPath& path);

bool Exists(const Stroka& serverName, const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

TOperationId StartOperation(
    const Stroka& serverName,
    const TTransactionId& transactionId,
    const Stroka& operationName,
    const Stroka& ysonSpec,
    bool wait);

void WaitForOperation(const Stroka& serverName, const TOperationId& operationId);

void AbortOperation(const Stroka& serverName, const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

Stroka ParseJsonString(const Stroka& response);

bool ParseBool(const Stroka& response);

TGUID ParseGuid(const Stroka& response);

void ParseStringArray(const Stroka& response, yvector<Stroka>& result);

////////////////////////////////////////////////////////////////////////////////

Stroka GetProxyForHeavyRequest(const Stroka& serverName);

Stroka RetryRequest(
    const Stroka& serverName,
    THttpHeader& header,
    const Stroka& body = "",
    bool isHeavy = false);

void RetryHeavyWriteRequest(
    const Stroka& serverName,
    const TTransactionId& parentId,
    THttpHeader& header,
    const TBuffer& buffer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
