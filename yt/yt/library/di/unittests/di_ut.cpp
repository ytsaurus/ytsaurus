#include <gtest/gtest.h>

#include <yt/yt/library/di/di.h>
#include <yt/yt/library/di/cycle_ptr.h>
#include <yt/yt/library/di/tagged_ptr.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NDI {
namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFoo)

struct TFoo
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(TFoo)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBar)

struct TBar
    : public TRefCounted
{
    YT_DI_CTOR(TBar(TFooPtr foo))
        : Foo(std::move(foo))
    { }

    TFooPtr Foo;
};

DEFINE_REFCOUNTED_TYPE(TBar)

////////////////////////////////////////////////////////////////////////////////

TEST(TInjector, Constructor)
{
    auto fooPtr = New<TFoo>();

    auto component = TComponent{}
        .BindInstance(fooPtr)
        .Bind<TBar>();

    TInjector injector{component};

    auto barPtr = injector.Get<TBarPtr>();
    ASSERT_EQ(barPtr->Foo, fooPtr);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBase)

struct TBase
    : public TRefCounted
{
    virtual void Method() = 0;
};

DEFINE_REFCOUNTED_TYPE(TBase)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TDerived)

struct TDerived
    : public TBase
{
    YT_DI_CTOR(TDerived()) = default;

    void Method() override
    { }
};

DEFINE_REFCOUNTED_TYPE(TDerived)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TZog)

struct TZog
    : public TRefCounted
{
    YT_DI_CTOR(TZog(TBasePtr base))
        : Base(std::move(base))
    { }

    TBasePtr Base;
};

DEFINE_REFCOUNTED_TYPE(TZog)

////////////////////////////////////////////////////////////////////////////////

TEST(TInjector, Upcast)
{
    auto component = TComponent{}
        .Bind<TDerived>()
        .UpcastFromTo<TDerived, TBase>()
        .Bind<TZog>();

    TInjector injector{component};

    auto zogPtr = injector.Get<TZogPtr>();
    zogPtr->Base->Method();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFirstCyclePart)
DECLARE_REFCOUNTED_STRUCT(TSecondCyclePart)

struct TFirstCyclePart
    : public TRefCounted
{
    YT_DI_CTOR(TFirstCyclePart(TSecondCyclePartPtr cycle))
        : Cycle(cycle)
    { }

    TSecondCyclePartPtr Cycle;
};

struct TSecondCyclePart
    : public TRefCounted
{
    YT_DI_CTOR(TSecondCyclePart(TFirstCyclePartPtr cycle))
        : Cycle(cycle)
    { }

    TFirstCyclePartPtr Cycle;
};

DEFINE_REFCOUNTED_TYPE(TFirstCyclePart)
DEFINE_REFCOUNTED_TYPE(TSecondCyclePart)

////////////////////////////////////////////////////////////////////////////////

TEST(TInjector, Cycle)
{
    auto component = TComponent{}
        .Bind<TFirstCyclePart>()
        .Bind<TSecondCyclePart>();

    try {
        TInjector injector{component};

        injector.Get<TFirstCyclePartPtr>();

        FAIL() << "Injector should detect cycle";
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSelfReference)

struct TSelfReference
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(TSelfReference)

TSelfReferencePtr CreateSelf(TInjector injector)
{
    return injector.Get<TSelfReferencePtr>();
}

TEST(TInjector, DynamicCycle)
{
    auto component = TComponent{}
        .Bind(CreateSelf);

    try {
        TInjector injector{component};

        injector.Get<TSelfReferencePtr>();

        FAIL() << "Injector should detect cycle";
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TServer)
DECLARE_REFCOUNTED_STRUCT(THandler)

struct TServer
    : public TRefCounted
{
    using THandlerCyclePtr = TCyclePtr<THandler>;

    YT_DI_CTOR(TServer(THandlerCyclePtr handler))
        : Handler(handler)
    { }

    THandlerCyclePtr Handler;
};

struct THandler
    : public TRefCounted
{
    YT_DI_CTOR(THandler(TServerPtr server))
        : Server(server)
    { }

    TServerPtr Server;
};

DEFINE_REFCOUNTED_TYPE(TServer)
DEFINE_REFCOUNTED_TYPE(THandler)

////////////////////////////////////////////////////////////////////////////////

TEST(TInjector, TwoPhaseConstruction)
{
    auto component = TComponent{}
        .Bind<TServer>()
        .Bind<THandler>();

    {
        TInjector injector{component};

        auto handler = injector.Get<THandlerPtr>();
        auto server = injector.Get<TServerPtr>();

        ASSERT_EQ(server.Get(), handler->Server.Get());
        ASSERT_EQ(handler.Get(), server->Handler.Get());

        server->Handler = {};
        handler->Server = {};
    }

    {
        TInjector injector{component};

        auto server = injector.Get<TServerPtr>();
        ASSERT_TRUE(server->Handler.Get());

        server->Handler = {};
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBug)

struct TBug
    : public TRefCounted
{
    TBug(TString)
    { }
};

DEFINE_REFCOUNTED_TYPE(TBug)

TEST(TInjector, CompilationError)
{
    // Uncomment this line to check that library gives clear compilation errors.
    // auto component = TComponent{}
    //     .Bind<TBug>();
}

////////////////////////////////////////////////////////////////////////////////

TFooPtr CreateFoo()
{
    return New<TFoo>();
}

TEST(TInjector, Function)
{
    auto component = TComponent{}
        .Bind(CreateFoo);

    TInjector injector{component};
    auto fooPtr = injector.Get<TFooPtr>();
    ASSERT_TRUE(fooPtr);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TFooPtr> CreateFooVector()
{
    return {
        New<TFoo>(),
        New<TFoo>(),
    };
}

TEST(TInjector, Vector)
{
    auto component = TComponent{}
        .Bind(CreateFooVector);
    
    TInjector injector{component};
    auto foos = injector.Get<std::vector<TFooPtr>>();
    ASSERT_EQ(2u, foos.size());
}

////////////////////////////////////////////////////////////////////////////////

TBarPtr CreateBar(TInjector injector)
{
    return New<TBar>(injector.Get<TFooPtr>());
}

TEST(TInjector, InjectInjector)
{
    auto component = TComponent{}
        .Bind(CreateBar)
        .BindInstance(New<TFoo>());

    TInjector injector{component};
    auto bar = injector.Get<TBarPtr>();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInvoker)

struct TInvoker
    : public TRefCounted
{
    YT_DI_CTOR(TInvoker()) = default;
};

DEFINE_REFCOUNTED_TYPE(TInvoker);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TLocation)

struct TLocation
    : public TRefCounted
{
    YT_DI_CTOR(TLocation(TInvokerPtr invoker))
        : Invoker(invoker)
    { }

    TInvokerPtr Invoker;
};

DEFINE_REFCOUNTED_TYPE(TLocation)

TComponent GetLocationComponent()
{
    return TComponent{}
        .Bind<TLocation>();
}

TLocationPtr NewLocation(TInjector injector)
{
    auto scope = injector.NewScope(GetLocationComponent());
    return scope.Get<TLocationPtr>();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInjector, NestedScope)
{
    auto component = TComponent{}
        .Bind(NewLocation)
        .Bind<TInvoker>();

    TInjector injector{component};

    auto location = injector.Get<TLocationPtr>();
    ASSERT_TRUE(location);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInvokerKind,
    (Control)
);

using TControlInvokerPtr = TTaggedPtr<TInvoker, EInvokerKind::Control>;

DECLARE_REFCOUNTED_STRUCT(TSlot)

struct TSlot
    : public TRefCounted
{
    YT_DI_CTOR(TSlot(
        TInvokerPtr invoker,
        TControlInvokerPtr controlInvoker))
        : Invoker(invoker)
        , ControlInvoker(controlInvoker)
    { }

    TInvokerPtr Invoker;
    TInvokerPtr ControlInvoker;
};

DEFINE_REFCOUNTED_TYPE(TSlot)

TEST(TInjector, TaggedValue)
{
    auto component = TComponent{}
        .Bind<TInvoker>()
        .Bind<TInvoker>(EInvokerKind::Control)
        .Bind<TSlot>();

    TInjector injector{component};

    auto slot = injector.Get<TSlotPtr>();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TServiceLocator)

struct TServiceLocator
    : public TRefCounted
{
    YT_DI_CTOR(TServiceLocator()) = default;

    TInvokerPtr Invoker = New<TInvoker>();

    TInvokerPtr GetInvoker() const
    {
        return Invoker;
    }
};

DEFINE_REFCOUNTED_TYPE(TServiceLocator);

TEST(TInjector, Method)
{
    auto component = TComponent{}
        .Bind<TServiceLocator>()
        .Bind(&TServiceLocator::GetInvoker);

    TInjector injector{component};

    auto locator = injector.Get<TServiceLocatorPtr>();
    auto invoker = injector.Get<TInvokerPtr>();

    ASSERT_EQ(locator->Invoker, invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDI
