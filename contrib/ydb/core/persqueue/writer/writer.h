#pragma once

#include <contrib/ydb/core/base/defs.h>
#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/core/grpc_services/local_rate_limiter.h>
#include <contrib/ydb/core/protos/msgbus.pb.h>
#include <contrib/ydb/core/protos/msgbus_pq.pb.h>
#include <contrib/ydb/core/persqueue/pq_rl_helpers.h>
#include <contrib/ydb/core/persqueue/write_id.h>
#include <contrib/ydb/core/kafka_proxy/kafka_producer_instance_id.h>

#include <variant>

#include "partition_chooser.h"

namespace NKikimr::NPQ {

struct TEvPartitionWriter {
    enum EEv {
        EvInitResult = EventSpaceBegin(TKikimrEvents::ES_PQ_PARTITION_WRITER),
        EvWriteRequest,
        EvWriteAccepted,
        EvWriteResponse,
        EvDisconnected,

        EvTxWriteRequest,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_PARTITION_WRITER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_PARTITION_WRITER)");

    struct TEvInitResult: public TEventLocal<TEvInitResult, EvInitResult> {
        using TSourceIdInfo = NKikimrClient::TPersQueuePartitionResponse::TCmdGetMaxSeqNoResult::TSourceIdInfo;

        struct TSuccess {
            TString OwnerCookie;
            TSourceIdInfo SourceIdInfo;
            TMaybe<TWriteId> WriteId;

            TString ToString() const;
        };

        struct TError {
            TString Reason;
            NKikimrClient::TResponse Response;

            TString ToString() const;
        };

        TString SessionId;
        TString TxId;
        std::variant<TSuccess, TError> Result;

        TEvInitResult(const TString& sessionId, const TString& txId,
                      const TString& ownerCookie, const TSourceIdInfo& sourceIdInfo, const TMaybe<TWriteId>& writeId)
            : SessionId(sessionId)
            , TxId(txId)
            , Result(TSuccess{ownerCookie, sourceIdInfo, writeId})
        {
        }

        TEvInitResult(const TString& sessionId, const TString& txId,
                      const TString& reason, NKikimrClient::TResponse&& response)
            : SessionId(sessionId)
            , TxId(txId)
            , Result(TError{reason, std::move(response)})
        {
        }

        bool IsSuccess() const { return Result.index() == 0; }
        const TSuccess& GetResult() const { return std::get<0>(Result); }
        const TError& GetError() const { return std::get<1>(Result); }
        TString ToString() const override;
    };

    struct TEvWriteRequest: public TEventPB<TEvWriteRequest, NKikimrClient::TPersQueueRequest, EvWriteRequest> {
        // Only Cookie & CmdWrite must be set, other fields can be overwritten
        TEvWriteRequest() = default;

        explicit TEvWriteRequest(ui64 cookie) {
            Record.MutablePartitionRequest()->SetCookie(cookie);
        }

        ui64 GetCookie() const {
            return Record.GetPartitionRequest().GetCookie();
        }
    };

    struct TEvWriteAccepted: public TEventLocal<TEvWriteAccepted, EvWriteAccepted> {
        TString SessionId;
        TString TxId;
        ui64 Cookie;

        TEvWriteAccepted(const TString& sessionId, const TString& txId, ui64 cookie)
            : SessionId(sessionId)
            , TxId(txId)
            , Cookie(cookie)
        {
        }

        TString ToString() const override;
    };

    struct TEvWriteResponse: public TEventPB<TEvWriteResponse, NKikimrClient::TResponse, EvWriteResponse> {
        enum class EErrorCode {
            InternalError,
            // Partition located on other node.
            PartitionNotLocal,
            // Partitition restarted.
            PartitionDisconnected,
            OverloadError,
        };

        struct TSuccess {
        };

        struct TError {
            EErrorCode Code;
            TString Reason;
        };

        TString SessionId;
        TString TxId;
        std::variant<TSuccess, TError> Result;

        TEvWriteResponse() = default;

        TEvWriteResponse(const TString& sessionId, const TString& txId,
                         NKikimrClient::TResponse&& response)
            : SessionId(sessionId)
            , TxId(txId)
            , Result(TSuccess{})
        {
            Record = std::move(response);
        }

        TEvWriteResponse(const TString& sessionId, const TString& txId,
                         const EErrorCode code, const TString& reason, NKikimrClient::TResponse&& response)
            : SessionId(sessionId)
            , TxId(txId)
            , Result(TError{code, reason})
        {
            Record = std::move(response);
        }

        bool IsSuccess() const { return Result.index() == 0; }
        const TError& GetError() const { return std::get<1>(Result); }
        TString DumpError() const;
        TString ToString() const override;
    };

    struct TEvDisconnected: public TEventLocal<TEvDisconnected, EvDisconnected> {
        TEvDisconnected(TEvWriteResponse::EErrorCode errorCode)
            : ErrorCode(errorCode) {
        }

        const TEvWriteResponse::EErrorCode ErrorCode;
    };

    struct TEvTxWriteRequest : public TEventLocal<TEvTxWriteRequest, EvTxWriteRequest> {
        TEvTxWriteRequest(const TString& sessionId, const TString& txId, THolder<TEvWriteRequest>&& request) :
            SessionId(sessionId),
            TxId(txId),
            Request(std::move(request))
        {
        }

        TString SessionId;
        TString TxId;
        THolder<TEvWriteRequest> Request;
    };

}; // TEvPartitionWriter


struct TPartitionWriterOpts {
    bool CheckState = false;
    bool AutoRegister = false;
    bool UseDeduplication = true;

    TString SourceId;
    std::optional<ui32> ExpectedGeneration;
    std::optional<ui64> InitialSeqNo;

    TString Database;
    TString TopicPath;
    TString Token;
    TString SessionId;
    TString TxId;
    // Used for deduplication in kafka idempotent producer (both in the transaction and out of the transaction)
    std::optional<NKafka::TProducerInstanceId> KafkaProducerInstanceId;
    // Indicates that this writer will write records in transactions
    std::optional<TString> KafkaTransactionalId;
    TString TraceId;
    TString RequestType;

    std::optional<NKikimrPQ::TPQTabletConfig::EMeteringMode> MeteringMode;
    TRlContext RlCtx;

    bool CheckRequestUnits() const { return RlCtx; }

    TPartitionWriterOpts& WithCheckState(bool value) { CheckState = value; return *this; }
    TPartitionWriterOpts& WithAutoRegister(bool value) { AutoRegister = value; return *this; }
    TPartitionWriterOpts& WithDeduplication(bool value) { UseDeduplication = value; return *this; }
    TPartitionWriterOpts& WithSourceId(const TString& value) { SourceId = value; return *this; }
    TPartitionWriterOpts& WithExpectedGeneration(ui32 value) { ExpectedGeneration = value; return *this; }
    TPartitionWriterOpts& WithExpectedGeneration(std::optional<ui32> value) { ExpectedGeneration = value; return *this; }
    TPartitionWriterOpts& WithCheckRequestUnits(const NKikimrPQ::TPQTabletConfig::EMeteringMode meteringMode , const TRlContext& rlCtx) { MeteringMode = meteringMode; RlCtx = rlCtx; return *this; }
    TPartitionWriterOpts& WithDatabase(const TString& value) { Database = value; return *this; }
    TPartitionWriterOpts& WithTopicPath(const TString& value) { TopicPath = value; return *this; }
    TPartitionWriterOpts& WithToken(const TString& value) { Token = value; return *this; }
    TPartitionWriterOpts& WithSessionId(const TString& value) { SessionId = value; return *this; }
    TPartitionWriterOpts& WithTxId(const TString& value) { TxId = value; return *this; }
    TPartitionWriterOpts& WithTraceId(const TString& value) { TraceId = value; return *this; }
    TPartitionWriterOpts& WithRequestType(const TString& value) { RequestType = value; return *this; }
    TPartitionWriterOpts& WithInitialSeqNo(const std::optional<ui64> value) { InitialSeqNo = value; return *this; }
    TPartitionWriterOpts& WithKafkaProducerInstanceId(const std::optional<NKafka::TProducerInstanceId>& value) { KafkaProducerInstanceId = value; return *this; }
    TPartitionWriterOpts& WithKafkaTransactionalId(const std::optional<TString>& value) { KafkaTransactionalId = value; return *this; }
};

IActor* CreatePartitionWriter(const TActorId& client,
                             // const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                              ui64 tabletId,
                              ui32 partitionId,
                              const TPartitionWriterOpts& opts = {});
}
