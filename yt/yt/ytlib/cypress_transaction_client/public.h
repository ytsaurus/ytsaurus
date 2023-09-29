#pragma once

namespace NYT::NCypressTransactionClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqStartTransaction;
class TRspStartTransaction;

class TReqCommitTransaction;
class TRspCommitTransaction;

class TReqAbortTransaction;
class TRspAbortTransaction;

class TReqPingTransaction;
class TRspPingTransaction;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressTransactionClient
