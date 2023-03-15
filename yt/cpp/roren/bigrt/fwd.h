#include <yt/cpp/roren/interface/fwd.h>

#include <bigrt/lib/processing/state_manager/base/fwd.h>
#include <bigrt/lib/processing/state_manager/generic/fwd.h>


namespace NYT {
    template <typename>
    class TIntrusivePtr;

    namespace NAuth {
        struct ITvmService;
        using ITvmServicePtr = NYT::TIntrusivePtr<ITvmService>;
    }
}

namespace google::protobuf
{
    class Message;
    class Descriptor;

    template <typename>
    class RepeatedPtrField;
}

namespace NBigRT {
    template <typename>
    class TWriterDescriptor;

    struct TMessageBatch;
    class TConsumingSystemConfig;
    class TStatelessShardProcessorConfig;
    class TSupplierConfig;
    class TYtQueueWriterConfig;

    class IYtQueueWriterFactory;
}

namespace NUserSessions::NRT
{
    class TLogbrokerWriterConfig;
    class TYtClientConfig;

    class ILogbrokerWriterFactory;
}

namespace NYTEx::NHttp {
    class TServerConfig;
}

namespace NBSYeti {
    class TTvmGlobalConfig;

    namespace NProfiling {
        class TExporterConfig;
    }
}

namespace NSFStats {
    class TSolomonContext;
}

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TBigRtMemoryWriteTransform;

class TBigRtProgramConfig;

class IBigRtExecutionContext;
using IBigRtExecutionContextPtr = ::TIntrusivePtr<IBigRtExecutionContext>;

class TTablePoller;
using TTablePollerPtr = ::TIntrusivePtr<TTablePoller>;

template <typename T>
class TClonablePool;

class TBigRtExecutorPool;
using TBigRtExecutorPoolPtr = ::TIntrusivePtr<TBigRtExecutorPool>;

struct TSerializedBlockConfig;

template <typename TKey, typename TState>
using TCreateStateManagerFunction = std::function<NBigRT::TGenericStateManagerPtr<TKey, TState>(ui64 shard, NSFStats::TSolomonContext)>;

class TBigRtStateConfig;

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IExecutionBlock;
using IExecutionBlockPtr = TIntrusivePtr<IExecutionBlock>;

class TCompositeBigRtWriter;
using TCompositeBigRtWriterPtr = NYT::TIntrusivePtr<TCompositeBigRtWriter>;

class TCompositeBigRtWriterFactory;
using TCompositeBigRtWriterFactoryPtr = NYT::TIntrusivePtr<TCompositeBigRtWriterFactory>;

class TStateManagerRegistry;
using TStateManagerRegistryPtr = TIntrusivePtr<TStateManagerRegistry>;

using TRegisterWriterFunction = std::function<void(TCompositeBigRtWriterFactory&, const NYT::NAuth::ITvmServicePtr&)>;
using TCreateBaseStateManagerFunction = std::function<NBigRT::TBaseStateManagerPtr(ui64 shard, const NSFStats::TSolomonContext&)>;

struct TBigRtStateManagerVtable;

struct TProcessorCommitContext;

using TOnCommitCallback = std::function<void(const TProcessorCommitContext&)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
