#pragma once
#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

// Seperate files for definitions are required to prevent jumping and inlining.

void Foo(NYT::TIntrusivePtr<NYT::NConcurrency::TThreadPool>& threadPool, int x);
void Bar(NYT::TIntrusivePtr<NYT::NConcurrency::TThreadPool>& threadPool, int x);
void AsyncStop(NYT::TIntrusivePtr<NYT::NConcurrency::TThreadPool>& threadPool);
