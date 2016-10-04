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

void Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const Stroka& type,
    bool ignoreExisting = true,
    bool recursive = false,
    const TMaybe<TNode>& attributes = Nothing());

void Remove(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    bool recursive = false,
    bool force = false);

void Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    bool ignoreExisting = true,
    bool recursive = false,
    const TMaybe<TNode>& attributes = Nothing());

void Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const Stroka& mode);

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const Stroka& response);

TGUID ParseGuidFromResponse(const Stroka& response);

TRichYPath CanonizePath(
    const TAuth& auth, const TRichYPath& path);

yvector<TRichYPath> CanonizePaths(
    const TAuth& auth, const yvector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

Stroka GetProxyForHeavyRequest(const TAuth& auth);

Stroka RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    const Stroka& body = "",
    bool isHeavy = false,
    bool isOperation = false,
    std::function<void()> errorCallback = {});

void RetryHeavyWriteRequest(
    const TAuth& auth,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<TInputStream>()> streamMaker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
