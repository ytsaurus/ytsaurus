#pragma once

#include "http.h"

#include <mapreduce/yt/interface/common.h>

#include <util/generic/maybe.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

struct TAuth
{
    TString ServerName;
    TString Token;
};

///////////////////////////////////////////////////////////////////////////////

TTransactionId StartTransaction(
    const TAuth& auth,
    const TTransactionId& parentId,
    const TMaybe<TDuration>& timeout,
    bool pingAncestors,
    const TMaybe<TString>& title,
    const TMaybe<TNode>& attributes);

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

TString Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path);

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TString& value);

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionid,
    const TYPath& path);

void Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TString& type,
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
    const TString& mode);

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const TString& response);

TGUID ParseGuidFromResponse(const TString& response);

TRichYPath CanonizePath(
    const TAuth& auth, const TRichYPath& path);

yvector<TRichYPath> CanonizePaths(
    const TAuth& auth, const yvector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TAuth& auth);

TString RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    const TString& body = "",
    bool isHeavy = false,
    bool isOperation = false);

void RetryHeavyWriteRequest(
    const TAuth& auth,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<IInputStream>()> streamMaker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
