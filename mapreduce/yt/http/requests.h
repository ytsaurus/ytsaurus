#pragma once

#include "http.h"

#include <mapreduce/yt/interface/common.h>

#include <util/generic/maybe.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

struct TAuth
{
    Stroka ServerName;
    Stroka Token;
};

///////////////////////////////////////////////////////////////////////////////

TTransactionId StartTransaction(
    const TAuth& auth,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout = Nothing(),
    bool pingAncestors = false,
    const TMaybe<TNode>& attributes = Nothing());

void PingTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId);

void AbortTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId);

void CommitTransaction(
    const TAuth& auth,
    const TTransactionId& transactionId);

////////////////////////////////////////////////////////////////////////////////

Stroka Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path);

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const Stroka& value);

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionid,
    const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

bool ParseBool(const Stroka& response);

TGUID ParseGuid(const Stroka& response);

////////////////////////////////////////////////////////////////////////////////

Stroka GetProxyForHeavyRequest(const TAuth& auth);

Stroka RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    const Stroka& body = "",
    bool isHeavy = false);

void RetryHeavyWriteRequest(
    const TAuth& auth,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<TInputStream>()> streamMaker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
