#include <library/cpp/xdelta3/state/data_ptr.h>
#include <library/cpp/xdelta3/state/create_proto.h>
#include <library/cpp/xdelta3/state/data_ptr.h>
#include <library/cpp/xdelta3/state/state.h>
#include <library/cpp/xdelta3/xdelta_codec/codec.h>

#include <contrib/python/py3c/py3c.h>

#include <util/generic/buffer.h>

#include <library/cpp/pybind/cast.h>
#include <library/cpp/pybind/ptr.h>
#include <library/cpp/pybind/v2.h>

#include <util/generic/yexception.h>
#include <util/system/env.h>

using namespace NXdeltaAggregateColumn;

namespace
{
    class TPyState {
        struct TArgs {
            TString Input;
        };

        TPyState(TArgs&& args)
            : Buffer(std::move(args.Input))
            , Impl(Arena, reinterpret_cast<const ui8*>(Buffer.Data()), Buffer.Size())
        {
        }

        TArgs MakeArgs(NPyBind::TPyObjectPtr args)
        {
            Y_ENSURE(PyBytes_Check(args.Get()));

            char* ptr = nullptr;
            Py_ssize_t size = 0;
            PyBytes_AsStringAndSize(args.Get(), &ptr, &size);
            return {
                .Input = TString{ptr, static_cast<size_t>(size)}
            };
        }

    public:
        TPyState(NPyBind::TPyObjectPtr args)
            : TPyState(MakeArgs(args))
        {
        }

        ~TPyState()
        {
        }

        size_t Type() const
        {
            return Impl.Type();
        }

        bool HasBaseHash() const
        {
            return Impl.Header().has_base_hash();
        }

        ui32 BaseHash() const
        {
            return Impl.Header().base_hash();
        }

        bool HasStateHash() const
        {
            return Impl.Header().has_state_hash();
        }

        ui32 StateHash() const
        {
            return Impl.Header().state_hash();
        }

        bool HasDataSize() const
        {
            return Impl.Header().has_data_size();
        }

        ui32 DataSize() const
        {
            return Impl.Header().data_size();
        }

        bool HasErrorCode() const
        {
            return Impl.Header().has_error_code();
        }

        size_t ErrorCode() const
        {
            return Impl.Error();
        }

        bool HasStateSize() const
        {
            return Impl.Header().has_state_size();
        }

        size_t StateSize() const
        {
            return Impl.Header().state_size();
        }

        NPyBind::TPyObjectPtr PayloadData() const
        {
            return NPyBind::TPyObjectPtr(PyBytes_FromStringAndSize(reinterpret_cast<const char*>(Impl.PayloadData()), Impl.PayloadSize()));
        }

        size_t PayloadSize() const
        {
            return Impl.PayloadSize();
        }

    private:
        NProtoBuf::Arena Arena;
        TString Buffer;
        TState Impl;
    };

    class TStateEncoder {
    public:
        TStateEncoder(NPyBind::TPyObjectPtr /*args*/)
        {
        }

        NPyBind::TPyObjectPtr CreateError(NPyBind::TPyObjectPtr args)
        {
            Y_ENSURE(PyInt_Check(args.Get()));
            auto error = PyInt_AsSsize_t(args.Get());
            auto buffer = EncodeErrorProtoAsString(static_cast<NXdeltaAggregateColumn::TStateHeader::EErrorCode>(error));
            return NPyBind::TPyObjectPtr(PyBytes_FromStringAndSize(buffer.Data(), buffer.Size()));
        }

        NPyBind::TPyObjectPtr CreateBase(NPyBind::TPyObjectPtr args)
        {
            Y_ENSURE(PyBytes_Check(args.Get()));
            auto buffer = EncodeBaseProtoAsString(reinterpret_cast<const ui8*>(PyBytes_AsString(args.Get())), PyBytes_Size(args.Get()));
            return NPyBind::TPyObjectPtr(PyBytes_FromStringAndSize(buffer.Data(), buffer.Size()));
        }

        NPyBind::TPyObjectPtr CreatePatch(NPyBind::TPyObjectPtr args)
        {
            Y_ENSURE(PyTuple_Check(args.Get()));
            Y_ENSURE(2 == PyTuple_Size(args.Get()));

            auto base = PyTuple_GetItem(args.Get(), 0);
            Y_ENSURE(PyBytes_Check(base));

            auto state = PyTuple_GetItem(args.Get(), 1);
            Y_ENSURE(PyBytes_Check(state));

            auto buffer = EncodePatchProtoAsString(
                reinterpret_cast<const ui8*>(PyBytes_AsString(base)),
                PyBytes_Size(base),
                reinterpret_cast<const ui8*>(PyBytes_AsString(state)),
                PyBytes_Size(state));
            return NPyBind::TPyObjectPtr(PyBytes_FromStringAndSize(buffer.Data(), buffer.Size()));
        }
    };

    class TXDeltaCodec {
    public:
        TXDeltaCodec(NPyBind::TPyObjectPtr /*args*/)
        {
        }

        NPyBind::TPyObjectPtr ComputePatch(NPyBind::TPyObjectPtr args)
        {
            Y_ENSURE(PyTuple_Check(args.Get()));
            Y_ENSURE(2 == PyTuple_Size(args.Get()));

            auto base = PyTuple_GetItem(args.Get(), 0);
            Y_ENSURE(PyBytes_Check(base));

            auto state = PyTuple_GetItem(args.Get(), 1);
            Y_ENSURE(PyBytes_Check(state));

            size_t patch_size = 0;
            auto patch = TDataPtr(NXdeltaAggregateColumn::ComputePatch(
                nullptr,
                reinterpret_cast<const ui8*>(PyBytes_AsString(base)),
                PyBytes_Size(base),
                reinterpret_cast<const ui8*>(PyBytes_AsString(state)),
                PyBytes_Size(state),
                &patch_size));

            return NPyBind::TPyObjectPtr(PyBytes_FromStringAndSize(reinterpret_cast<const char*>(patch.get()), patch_size));
        }

        NPyBind::TPyObjectPtr ApplyPatch(NPyBind::TPyObjectPtr args)
        {
            Y_ENSURE(PyTuple_Check(args.Get()));
            Y_ENSURE(3 == PyTuple_Size(args.Get()));

            auto base = PyTuple_GetItem(args.Get(), 0);
            Y_ENSURE(PyBytes_Check(base));

            auto patch = PyTuple_GetItem(args.Get(), 1);
            Y_ENSURE(PyBytes_Check(patch));

            auto decoded_size = PyTuple_GetItem(args.Get(), 2);
            Y_ENSURE(PyInt_Check(decoded_size));

            size_t state_size = 0;
            auto state = TDataPtr(NXdeltaAggregateColumn::ApplyPatch(
                nullptr,
                0,
                reinterpret_cast<const ui8*>(PyBytes_AsString(base)),
                PyBytes_Size(base),
                reinterpret_cast<const ui8*>(PyBytes_AsString(patch)),
                PyBytes_Size(patch),
                PyInt_AsSsize_t(decoded_size),
                &state_size));

            return NPyBind::TPyObjectPtr(PyBytes_FromStringAndSize(reinterpret_cast<const char*>(state.get()), state_size));
        }
    };
}

void ExportState()
{
    ::NPyBind::TPyClass<TPyState, NPyBind::TPyClassConfigTraits<true>, NPyBind::TPyObjectPtr&>("State")
        .AsProperty("type", &TPyState::Type)
        .AsProperty("has_base_hash", &TPyState::HasBaseHash)
        .AsProperty("base_hash", &TPyState::BaseHash)
        .AsProperty("has_state_hash", &TPyState::HasStateHash)
        .AsProperty("state_hash", &TPyState::StateHash)
        .AsProperty("has_state_size", &TPyState::HasStateSize)
        .AsProperty("state_size", &TPyState::StateSize)
        .AsProperty("has_data_size", &TPyState::HasDataSize)
        .AsProperty("data_size", &TPyState::DataSize)
        .AsProperty("has_error_code", &TPyState::HasErrorCode)
        .AsProperty("error_code", &TPyState::ErrorCode)
        .AsProperty("has_base_hash", &TPyState::HasBaseHash)
        .AsProperty("base_hash", &TPyState::BaseHash)
        .AsProperty("payload_data", &TPyState::PayloadData)
        .AsProperty("payload_size", &TPyState::PayloadSize)
        .AsPropertyByFunc("BASE_TYPE", [](const TPyState&) { return TStateHeader::BASE; })
        .AsPropertyByFunc("PATCH_TYPE", [](const TPyState&) { return TStateHeader::PATCH; })
        .AsPropertyByFunc("ERROR_TYPE", [](const TPyState&) { return TStateHeader::NONE_TYPE; })
        .Complete();

    ::NPyBind::TPyClass<TStateEncoder, NPyBind::TPyClassConfigTraits<true>, NPyBind::TPyObjectPtr&>("StateEncoder")
        .Def("create_error_state", &TStateEncoder::CreateError)
        .Def("create_base_state", &TStateEncoder::CreateBase)
        .Def("create_patch_state", &TStateEncoder::CreatePatch)
        .Complete();

    ::NPyBind::TPyClass<TXDeltaCodec, NPyBind::TPyClassConfigTraits<true>, NPyBind::TPyObjectPtr&>("XDeltaCodec")
        .Def("compute_patch", &TXDeltaCodec::ComputePatch)
        .Def("apply_patch", &TXDeltaCodec::ApplyPatch)
        .Complete();
}
