#include "yt_wrapper.h"

#include <contrib/ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/digest/md5/md5.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/stream/fwd.h>
#include <util/stream/buffered.h>
#include <util/system/mutex.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/file_reader.h>

#include <library/cpp/yt/string/guid.h>

#include <yt/yt/core/ytree/convert.h>

using namespace NYql;
using namespace NYT;
using namespace NYT::NApi;
using namespace NActors;

namespace NYql {
    struct TRequest: public NYT::TRefCounted {
        const TActorId SelfId;
        const TActorId Sender;
        TActorSystem* Ctx;
        const ui64 RequestId;

        TRequest(const TActorId& selfId, const TActorId& sender, TActorSystem* ctx, const ui64 requestId)
            : SelfId(selfId)
            , Sender(sender)
            , Ctx(ctx)
            , RequestId(requestId)
        { }

        void Complete(IEventBase* ev);
    };

    struct TWriteFileRequest: public TRequest {
        IClientPtr Client;
        IFileWriterPtr FileWriter;
        TFile File;
        i64 FileSize = -1;
        TVector<char> Buffer;
        TString NodePathTmp;
        TString NodePath;
        TFileWriterOptions WriterOptions;
        TString Digest;
        i64 Offset = 0;

        TWriteFileRequest(const TActorId& selfId, const TActorId& sender, TActorSystem* ctx, const ui64 requestId)
            : TRequest(selfId, sender, ctx, requestId)
        { }


        TFuture<void> WriteNext() {
            const ui64 chunkSize = 64 * 1024 * 1024;
            Buffer.resize(chunkSize);

            i64 dataSize;
            try {
                if (FileSize < 0) {
                    FileSize = File.GetLength();
                }
                dataSize = File.Pread(&Buffer[0], Buffer.size(), Offset);
                Offset += dataSize;
            } catch (const std::exception& ex) {
                return MakeFuture(TErrorOr<void>(ex));
            }

            if (dataSize == 0) {
                YQL_CLOG(DEBUG, ProviderDq) << "WriteNext: all bytes written (total=" << Offset
                    << "), closing writer for " << NodePathTmp;
                return FileWriter->Close()
                    .Apply(BIND([self = MakeWeak(this)]() {
                        auto this_ = self.Lock();
                        if (!this_) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }

                        return this_->Client->GetNode(this_->NodePathTmp + "/@md5")
                            .Apply(BIND([self](const TErrorOr<NYT::NYson::TYsonString>& err) {
                            auto req = self.Lock();
                            if (!req) {
                                return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                            }
                            if (err.IsOK() && req->Digest == NYTree::ConvertTo<TString>(err.Value())) {
                                YQL_CLOG(DEBUG, ProviderDq) << "WriteNext: checksum verified for " << req->NodePathTmp;
                                return OKFuture;
                            }

                            auto remoteDigest = err.IsOK() ? NYTree::ConvertTo<TString>(err.Value()) : TString("<error>");
                            YQL_CLOG(ERROR, ProviderDq) << "WriteNext: checksum mismatch for " << req->NodePathTmp
                                << " local=" << req->Digest << " remote=" << remoteDigest;
                            return MakeFuture(TErrorOr<void>(yexception() << "wrong checksum"));
                        }));
                    }));
            } else {
                YQL_CLOG(DEBUG, ProviderDq) << "Writing chunk " << Offset << "/" << FileSize;
                return FileWriter->Write(TSharedRef(&Buffer[0], dataSize, nullptr))
                    .Apply(BIND([self = MakeWeak(this)]() mutable {
                        auto this_ = self.Lock();
                        if (!this_) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }
                        return this_->WriteNext();
                    }).AsyncVia(Client->GetConnection()->GetInvoker()));
            }
        }

        TFuture<void> WriteFile()
        {
            auto& remotePath = NodePathTmp;
            if (FileSize < 0) {
                FileSize = File.GetLength();
            }
            YQL_CLOG(DEBUG, ProviderDq) << "WriteFile: opening writer for " << remotePath
                << " fileSize=" << FileSize;
            FileWriter = Client->CreateFileWriter(remotePath, WriterOptions);

            return FileWriter->Open().Apply(BIND([self = MakeWeak(this)]() mutable {
                auto this_ = self.Lock();
                if (!this_) {
                    return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                }
                return this_->WriteNext();
            }));
        }
    };

    class TReadFileRequest: public TRequest {
    public:
        TReadFileRequest(const TActorId& selfId, const TActorId& sender, TActorSystem* ctx, const ui64 requestId)
            : TRequest(selfId, sender, ctx, requestId)
        { }

        void Init(IClientPtr client, TString remotePath, TString localPath) {
            Client_ = std::move(client);
            RemotePath_ = std::move(remotePath);
            LocalPath_ = std::move(localPath);
        }

        void SetDigest(TString digest) {
            Digest_ = std::move(digest);
        }

        void SetExpectedUncompressedSize(i64 size) {
            ExpectedUncompressedSize_ = size;
        }

        void SetReader(IFileReaderPtr reader) {
            Reader_ = std::move(reader);
        }

        const TString& GetLocalPath() const {
            return LocalPath_;
        }

        IClientPtr GetClient() const {
            return Client_;
        }

        TFuture<void> ReadFile() {
            return BIND([self = MakeWeak(this)]() {
                DownloadToLocal(self);
            }).AsyncVia(Client_->GetConnection()->GetInvoker()).Run();
        }

    private:
        static void DownloadToLocal(TWeakPtr<TReadFileRequest> request) {
            TString remotePath;
            TString localPath;
            TString digest;
            i64 expectedSize = 0;

            auto lockOrThrow = [&](i64 downloadedBytes = 0) {
                auto locked = request.Lock();
                if (!locked) {
                    throw yexception()
                        << "read interrupted for remote=" << remotePath
                        << " local=" << localPath
                        << ", downloaded " << downloadedBytes << "/" << expectedSize << " bytes";
                }
                return locked;
            };

            {
                const auto req = lockOrThrow();
                remotePath = req->RemotePath_;
                localPath = req->LocalPath_;
                digest = req->Digest_;
                expectedSize = req->ExpectedUncompressedSize_;
            }

            const auto pos = localPath.rfind('/');
            if (pos != TString::npos) {
                const auto dirName = localPath.substr(0, pos);
                if (!dirName.empty()) {
                    NFs::MakeDirectoryRecursive(dirName, NFs::FP_NONSECRET_FILE, false);
                }
            }

            TFileOutput output(localPath);
            MD5 md5;
            i64 bytesDownloaded = 0;

            auto formatDownloadProgress = [&](i64 downloaded) {
                if (expectedSize <= 0) {
                    return ::TStringBuilder() << downloaded << " bytes";
                }
                const i64 remaining = expectedSize - downloaded;
                const i64 percent = downloaded * 100 / expectedSize;
                if (remaining > 0) {
                    return ::TStringBuilder() << percent << "% (" << downloaded << "/" << expectedSize
                        << ", " << remaining << " left)";
                }
                if (remaining == 0) {
                    return ::TStringBuilder() << "100% (" << downloaded << "/" << expectedSize << ")";
                }
                return ::TStringBuilder() << percent << "% (" << downloaded << "/" << expectedSize
                    << ", +" << (-remaining) << " over expected)";
            };

            while (true) {
                TFuture<TSharedRef> part = lockOrThrow(bytesDownloaded)->Reader_->Read();
                auto blob = NYT::NConcurrency::WaitFor(part).ValueOrThrow();

                if (blob.Size() == 0) {
                    if (expectedSize > 0 && bytesDownloaded < expectedSize) {
                        throw yexception()
                            << "size mismatch for " << remotePath
                            << " (local path " << localPath << ")"
                            << ": expected at least " << expectedSize
                            << " bytes, downloaded " << bytesDownloaded << " bytes";
                    }
                    if (expectedSize > 0 && bytesDownloaded > expectedSize) {
                        YQL_CLOG(WARN, ProviderDq) << "ReadFile: downloaded more than expected for "
                            << remotePath << ": " << formatDownloadProgress(bytesDownloaded);
                    }

                    char digestBuf[33];
                    const TString computedDigest{md5.End(digestBuf)};

                    if (computedDigest == digest) {
                        YQL_CLOG(INFO, ProviderDq) << "ReadFile complete: "
                            << formatDownloadProgress(bytesDownloaded)
                            << " md5=" << computedDigest
                            << " remote=" << remotePath << " local=" << localPath;
                        return;
                    }

                    throw yexception()
                        << "md5 mismatch for " << remotePath
                        << " (local path " << localPath << ")"
                        << ": expected " << digest
                        << ", got " << computedDigest
                        << ", downloaded " << bytesDownloaded
                        << (expectedSize > 0 ? ::TStringBuilder() << "/" << expectedSize : TString())
                        << " bytes";
                }

                md5.Update(blob.Begin(), blob.Size());
                output.Write(blob.Begin(), blob.Size());
                bytesDownloaded += blob.Size();

                YQL_CLOG(DEBUG, ProviderDq) << "ReadFile progress: "
                    << formatDownloadProgress(bytesDownloaded)
                    << " chunk=" << blob.Size()
                    << " remote=" << remotePath;
            }
        }

    private:
        IClientPtr Client_;
        TString RemotePath_;
        TString LocalPath_;
        IFileReaderPtr Reader_;
        TString Digest_;
        i64 ExpectedUncompressedSize_ = 0;
    };

    using TRequestPtr = NYT::TIntrusivePtr<TRequest>;

    struct TEvComplete
        : NActors::TEventLocal<TEvComplete, TDqEvents::ES_OTHER1> {
        TEvComplete() = default;
        explicit TEvComplete(const TRequestPtr& req)
            : Request(req)
        { }

        const TRequestPtr Request;
    };

    void TRequest::Complete(IEventBase* ev) {
        Ctx->Send(Sender, ev);
        Ctx->Send(SelfId, new TEvComplete(NYT::MakeStrong(this)));
    }

    class TYtWrapper: public TActor<TYtWrapper> {
    public:
        static constexpr char ActorName[] = "YT_WRAPPER";

        TYtWrapper(const IClientPtr& client, const TString& clusterName)
            : TActor(&TYtWrapper::Handler)
            , Client(client)
            , ClusterName(clusterName)
        { }

    private:
        STRICT_STFUNC(Handler, {
            HFunc(TEvStartOperation, OnStartOperation)
            HFunc(TEvGetOperation, OnGetOperation)
            HFunc(TEvListOperations, OnListOperations)
            HFunc(TEvGetJob, OnGetJob)
            HFunc(TEvWriteFile, OnFileWrite)
            HFunc(TEvReadFile, OnReadFile)
            HFunc(TEvListNode, OnListNode)
            HFunc(TEvSetNode, OnSetNode)
            HFunc(TEvGetNode, OnGetNode)
            HFunc(TEvRemoveNode, OnRemoveNode)
            HFunc(TEvCreateNode, OnCreateNode)
            HFunc(TEvStartTransaction, OnStartTransaction)
            HFunc(TEvPrintJobStderr, OnPrintJobStderr)
            HFunc(TEvComplete, OnComplete)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
        });

        THashSet<TRequestPtr> Requests;

        void PassAway() override {
            Requests.clear();
            IActor::PassAway();
        }

        template<typename T>
        TWeakPtr<T> NewRequest(ui64 id, TActorId sender, const TActorContext& ctx) {
            auto req = New<T>(SelfId(), sender, ctx.ActorSystem(), id);
            Requests.emplace(req);
            return NYT::MakeWeak(req);
        }

        void OnComplete(TEvComplete::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto req = ev->Get()->Request;
            Requests.erase(req);
        }

        void OnFileWrite(TEvWriteFile::TPtr& ev, const TActorContext& ctx) {
            YQL_LOG_CTX_ROOT_SCOPE(ClusterName);
            TFile file = std::move(std::get<0>(*ev->Get()));
            NYPath::TRichYPath remotePath = std::get<1>(*ev->Get());
            THashMap<TString, NYT::TNode> attributes = std::get<2>(*ev->Get());
            TFileWriterOptions writerOptions = std::get<3>(*ev->Get());
            auto requestId = ev->Get()->RequestId;

            auto nodePathTmp = remotePath.GetPath() + ".tmp";
            auto nodePath = remotePath.GetPath();
            auto request = NewRequest<TWriteFileRequest>(requestId, ev->Sender, ctx);
            writerOptions.ComputeMD5 = true;

            try {
                Y_ENSURE(file.IsOpen());

                i64 localFileSize = file.GetLength();
                TString digest;

                if (writerOptions.ComputeMD5) {
                    char buf[32768];
                    MD5 md5;
                    i64 size, offset = 0;
                    auto md5Start = TInstant::Now();
                    while ((size = file.Pread(buf, sizeof(buf), offset)) > 0) {
                        md5.Update(buf, size);
                        offset += size;
                    }
                    char digestBuf[33];
                    digest = md5.End(digestBuf);
                    YQL_CLOG(DEBUG, ProviderDq) << "Local MD5 for " << nodePath << ": " << digest
                        << " elapsed=" << (TInstant::Now() - md5Start).Seconds() << " sec"
                        << " size=" << offset;
                }

                YQL_CLOG(INFO, ProviderDq) << "OnFileWrite path=" << nodePath
                    << " localSize=" << localFileSize << " digest=" << digest;

                if (auto req = request.Lock()) {
                    req->Client = Client;
                    req->File = std::move(file);
                    req->NodePathTmp = nodePathTmp;
                    req->NodePath = nodePath;
                    req->WriterOptions = writerOptions;
                    req->Digest = digest;
                }

                auto logCtx = NYql::NLog::CurrentLogContextPath();
                YT_UNUSED_FUTURE(Client->GetNode(nodePath + "/@md5")
                    .Apply(BIND([request, attributes, digest, localFileSize, logCtx](const TErrorOr<NYT::NYson::TYsonString>& err) mutable {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                        auto req = request.Lock();
                        if (!req) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }
                        if (err.IsOK() && digest == NYTree::ConvertTo<TString>(err.Value())) {
                            YQL_CLOG(DEBUG, ProviderDq) << "File already uploaded: " << req->NodePath;
                            try {
                                YT_UNUSED_FUTURE(req->Client->SetNode(req->NodePath + "/@yql_last_update",
                                    NYT::NYson::TYsonString(
                                        NYT::NodeToYsonString(NYT::TNode(ToString(TInstant::Now()))
                                    ))));
                            } catch (...) { }
                            return OKFuture;
                        } else if (err.IsOK() || err.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                            TCreateNodeOptions options;
                            options.Recursive = true;
                            options.IgnoreExisting = true;

                            if (err.IsOK()) {
                                YQL_CLOG(ERROR, ProviderDq) << "MD5 mismatch for " << req->NodePath
                                    << ": local=" << digest
                                    << " remote=" << NYTree::ConvertTo<TString>(err.Value());
                            } else {
                                YQL_CLOG(INFO, ProviderDq) << "Node not found, will upload: " << req->NodePath
                                    << " (" << ToString(err) << ")"
                                    << " local=" << digest
                                    << " localSize=" << localFileSize
                                    << " creating tmp node " << req->NodePathTmp
                                    << " re-uploading";
                            }

                            return req->Client->CreateNode(req->NodePathTmp, NObjectClient::EObjectType::File, options).As<void>()
                                .Apply(BIND([request, attributes] () {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    YQL_CLOG(DEBUG, ProviderDq) << "Tmp node created: " << req->NodePathTmp
                                        << ", setting " << (attributes.size() + 1) << " attributes";
                                    TVector<NYT::TFuture<void>> futures;
                                    futures.reserve(attributes.size() + 1);
                                    for (const auto& [k, v]: attributes) {
                                        futures.push_back(
                                            req->Client->SetNode(
                                                req->NodePathTmp + "/@" + k,
                                                NYT::NYson::TYsonString(NYT::NodeToYsonString(v)),
                                                NYT::NApi::TSetNodeOptions()));
                                    }
                                    futures.push_back(
                                        req->Client->SetNode(
                                            req->NodePathTmp + "/@expiration_timeout",
                                            NYT::NYson::TYsonString(NYT::NodeToYsonString(NYT::TNode(86400000))), // 24 hours
                                            NYT::NApi::TSetNodeOptions()));
                                    return NYT::AllSucceeded(futures).As<void>();
                                }))
                                .Apply(BIND([request]() mutable {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    YQL_CLOG(DEBUG, ProviderDq) << "Attributes set, starting file write to " << req->NodePathTmp;
                                    return req->WriteFile();
                                }))
                                .Apply(BIND([request] () {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    YQL_CLOG(DEBUG, ProviderDq) << "File written, moving " << req->NodePathTmp << " -> " << req->NodePath;
                                    auto moveOptions = NYT::NApi::TMoveNodeOptions();
                                    moveOptions.Force = true;
                                    return req->Client->MoveNode(req->NodePathTmp, req->NodePath, moveOptions).As<void>();
                                }))
                                .Apply(BIND([request] () {
                                    auto req = request.Lock();
                                    if (!req) {
                                        return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                    }
                                    YQL_CLOG(INFO, ProviderDq) << "File upload complete: " << req->NodePath;
                                    auto removeOptions = NYT::NApi::TRemoveNodeOptions();
                                    removeOptions.Force = true;
                                    return req->Client->RemoveNode(req->NodePath + "/@expiration_timeout", removeOptions).As<void>();
                                }));
                        }

                        YQL_CLOG(WARN, ProviderDq) << "GetNode @md5 unexpected error for " << req->NodePath
                            << ": " << ToString(err);
                        err.ThrowOnError();

                        return OKFuture;
                    }))
                    .Apply(BIND([request, requestId](const TErrorOr<void>& err)
                    {
                        if (auto req = request.Lock()) {
                            if (!err.IsOK()) {
                                YQL_CLOG(WARN, ProviderDq) << "OnFileWrite failed for " << req->NodePath
                                    << ": " << ToString(err);
                            }
                            req->Complete(new TEvWriteFileResponse(requestId, err));
                        }
                    })));
            } catch (const std::exception& ex) {
                YQL_CLOG(WARN, ProviderDq) << "OnFileWrite exception for " << nodePath << ": " << ex.what();
                if (auto req = request.Lock()) {
                    req->Complete(new TEvWriteFileResponse(requestId, ex));
                }
            }
        }

        void OnReadFile(TEvReadFile::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TReadFileRequest>(requestId, ev->Sender, ctx);

            try {
                NYPath::TRichYPath remotePath = std::get<0>(*ev->Get());
                TFileReaderOptions readerOptions = std::get<2>(*ev->Get());
                const auto nodePath = remotePath.GetPath();
                if (auto req = request.Lock()) {
                    req->Init(Client, nodePath, std::get<1>(*ev->Get()));
                }

                YQL_CLOG(DEBUG, ProviderDq) << "OnReadFile remote=" << nodePath;
                NYT::NApi::TGetNodeOptions getNodeOptions;
                getNodeOptions.Attributes = NYTree::TAttributeFilter({"md5", "uncompressed_data_size"});
                YT_UNUSED_FUTURE(Client->GetNode(nodePath + "/@", getNodeOptions)
                    .Apply(BIND([request, nodePath, readerOptions](const TErrorOr<NYT::NYson::TYsonString>& err) mutable {
                        auto req = request.Lock();
                        if (!req) {
                            return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                        }
                        if (!err.IsOK()) {
                            YQL_CLOG(WARN, ProviderDq) << "ReadFile: GetNode failed for " << nodePath
                                << ": " << ToString(err);
                            return MakeFuture(TErrorOr<void>(yexception()
                                << "failed to get file attributes for " << nodePath << ": " << ToString(err)));
                        }
                        const auto attributes = NYTree::ConvertToAttributes(err.Value());
                        if (!attributes->Contains("md5") || !attributes->Contains("uncompressed_data_size")) {
                            YQL_CLOG(WARN, ProviderDq) << "ReadFile: missing attributes for " << nodePath
                                << ": " << err.Value().AsStringBuf();
                            return MakeFuture(TErrorOr<void>(yexception()
                                << "missing md5 or uncompressed_data_size attributes for " << nodePath
                                << ", got " << err.Value().AsStringBuf()));
                        }
                        const auto digest = attributes->Get<TString>("md5");
                        const auto expectedSize = attributes->Get<i64>("uncompressed_data_size");
                        req->SetDigest(digest);
                        req->SetExpectedUncompressedSize(expectedSize);
                        YQL_CLOG(DEBUG, ProviderDq) << "ReadFile: expected md5=" << digest
                            << " uncompressed_data_size=" << expectedSize
                            << " remote=" << nodePath << " local=" << req->GetLocalPath();

                        return req->GetClient()->CreateFileReader(nodePath, readerOptions)
                            .Apply(BIND([request](const IFileReaderPtr& reader) {
                                auto req = request.Lock();
                                if (!req) {
                                    return MakeFuture(TErrorOr<void>(yexception() << "request complete"));
                                }
                                req->SetReader(reader);
                                return req->ReadFile();
                            }));
                    }))
                    .Apply(BIND([request, requestId](const TErrorOr<void>& err) {
                        if (auto req = request.Lock()) {
                            req->Complete(new TEvReadFileResponse(requestId, err));
                        }
                    })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvReadFileResponse(requestId, ex));
                }
            }
        }

        void OnStartOperation(TEvStartOperation::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                NScheduler::EOperationType type = std::get<0>(*ev->Get());
                auto spec = NYT::NYson::TYsonString(std::get<1>(*ev->Get()));
                TStartOperationOptions options = std::get<2>(*ev->Get());

                Client->StartOperation(type, spec, options).Subscribe(BIND([=](const TErrorOr<NScheduler::TOperationId>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvStartOperationResponse(requestId, result));
                    }
                }));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvStartOperationResponse(requestId, ex));
                }
            }
        }

        void OnGetOperation(TEvGetOperation::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                auto operationId = std::get<0>(*ev->Get());
                auto options = std::get<1>(*ev->Get());

                YT_UNUSED_FUTURE(Client->GetOperation(operationId, options).Apply(BIND([=](const TErrorOr<TOperation>& result) {
                    return NYT::NYson::ConvertToYsonString(result.ValueOrThrow()).ToString();
                }))
                .Apply(BIND([=](const TErrorOr<TString>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvGetOperationResponse(requestId, result));
                    }
                })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvGetOperationResponse(requestId, ex));
                }
            }
        }

        void OnListOperations(TEvListOperations::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                auto options = std::get<0>(*ev->Get());

                YT_UNUSED_FUTURE(Client->ListOperations(options).Apply(BIND([=](const TErrorOr<NYT::NApi::TListOperationsResult>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvListOperationsResponse(requestId, result));
                    }
                })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvListOperationsResponse(requestId, ex));
                }
            }
        }

        void OnGetJob(TEvGetJob::TPtr& ev, const TActorContext& ctx) {
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                auto operationId = std::get<0>(*ev->Get());
                auto jobId = std::get<1>(*ev->Get());
                auto options = std::get<2>(*ev->Get());

                YT_UNUSED_FUTURE(Client->GetJob(operationId, jobId, options).Apply(BIND([=](const TErrorOr<NYT::NYson::TYsonString>& result) {
                    return result.ValueOrThrow().ToString();
                }))
                .Apply(BIND([=](const TErrorOr<TString>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvGetJobResponse(requestId, result));
                    }
                })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvGetJobResponse(requestId, ex));
                }
            }
        }

        void OnListNode(TEvListNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            try {
                YT_UNUSED_FUTURE(Client->ListNode(path, options)
                    .Apply(BIND([=](const TErrorOr<NYT::NYson::TYsonString>& result) {
                        return result.ValueOrThrow().ToString();
                    }))
                    .Apply(BIND([=](const TErrorOr<TString>& result) {
                        if (auto req = request.Lock()) {
                            req->Complete(new TEvListNodeResponse(requestId, result));
                        }
                    })));
            } catch (const std::exception& ex) {
                if (auto req = request.Lock()) {
                    req->Complete(new TEvListNodeResponse(requestId, ex));
                }
            }
        }

        void OnSetNode(TEvSetNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto value = std::get<1>(*ev->Get());
            auto options = std::get<2>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->SetNode(path, value, options)
                .Apply(BIND([=](const TErrorOr<void>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvSetNodeResponse(requestId, result));
                    }
                })));
        }

        void OnGetNode(TEvGetNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->GetNode(path, options)
                .Apply(BIND([=](const TErrorOr<NYT::NYson::TYsonString>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvGetNodeResponse(requestId, result));
                    }
                })));
        }

        void OnRemoveNode(TEvRemoveNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YT_UNUSED_FUTURE(Client->RemoveNode(path, options)
                .Apply(BIND([=](const TErrorOr<void>& result) {
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvRemoveNodeResponse(requestId, result));
                    }
                })));
        }

        void OnCreateNode(TEvCreateNode::TPtr& ev, const TActorContext& ctx) {
            auto path = std::get<0>(*ev->Get());
            auto type = std::get<1>(*ev->Get());
            auto options = std::get<2>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YQL_CLOG(DEBUG, ProviderDq) << "YtWrapper CreateNode request"
                << " cluster=" << ClusterName
                << " path=" << path;

            YT_UNUSED_FUTURE(Client->CreateNode(path, type, options)
                .Apply(BIND([=](const TErrorOr<NYT::NCypressClient::TNodeId>& result) {
                    if (result.IsOK()) {
                        const auto nodeId = result.Value();
                        YQL_CLOG(DEBUG, ProviderDq) << "YtWrapper CreateNode OK"
                            << " cluster=" << ClusterName
                            << " path=" << path
                            << " node_id=" << ToString(nodeId);
                    } else {
                        YQL_CLOG(WARN, ProviderDq) << "YtWrapper CreateNode failed"
                            << " cluster=" << ClusterName
                            << " path=" << path
                            << " error=" << ToString(result);
                    }
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvCreateNodeResponse(requestId, result));
                    }
                })));
        }

        void OnStartTransaction(TEvStartTransaction::TPtr& ev, const TActorContext& ctx) {
            auto type = std::get<0>(*ev->Get());
            auto options = std::get<1>(*ev->Get());
            auto requestId = ev->Get()->RequestId;
            auto request = NewRequest<TRequest>(requestId, ev->Sender, ctx);

            YQL_CLOG(DEBUG, ProviderDq) << "YtWrapper StartTransaction request"
                << " cluster=" << ClusterName;

            YT_UNUSED_FUTURE(Client->StartTransaction(type, options)
                .Apply(BIND([=](const TErrorOr<ITransactionPtr>& result) {
                    if (result.IsOK()) {
                        const auto txId = result.Value()->GetId();
                        YQL_CLOG(DEBUG, ProviderDq) << "YtWrapper StartTransaction OK"
                            << " cluster=" << ClusterName
                            << " tx=" << ToString(txId);
                    } else {
                        YQL_CLOG(WARN, ProviderDq) << "YtWrapper StartTransaction failed"
                            << " cluster=" << ClusterName
                            << " error=" << ToString(result);
                    }
                    if (auto req = request.Lock()) {
                        req->Complete(new TEvStartTransactionResponse(requestId, result));
                    }
                })));
        }

        void OnPrintJobStderr(TEvPrintJobStderr::TPtr& ev, const TActorContext& ctx) {
            YQL_LOG_CTX_ROOT_SCOPE(ClusterName);
            Y_UNUSED(ctx);
            auto operationId = std::get<0>(*ev->Get());

            YQL_CLOG(DEBUG, ProviderDq) << "Printing stderr of operation " << ToString(operationId);

            auto logCtx = NYql::NLog::CurrentLogContextPath();
            YT_UNUSED_FUTURE(Client->ListJobs(operationId)
                .Apply(BIND([operationId, client = MakeWeak(Client), logCtx](const TListJobsResult& result) {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                    if (auto cli = client.Lock()) {
                        for (const auto& job : result.Jobs) {
                            YQL_CLOG(DEBUG, ProviderDq) << "Printing stderr (" << ToString(operationId) << "," << ToString(job.Id) << ")";

                            YT_UNUSED_FUTURE(cli->GetJobStderr(operationId, job.Id)
                                .Apply(BIND([jobId = job.Id, operationId, logCtx](const TGetJobStderrResponse& response) {
                                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                                    YQL_CLOG(DEBUG, ProviderDq)
                                        << "Stderr ("
                                        << ToString(operationId) << ","
                                        << ToString(jobId) << ")"
                                        << TString(response.Data.Begin(), response.Data.Size());
                                })));
                        }
                    }
                })));
        }

        IClientPtr Client;
        TString ClusterName;
    };

    IActor* CreateYtWrapper(const IClientPtr& client, const TString& clusterName) {
        return new TYtWrapper(client, clusterName);
    }
}
