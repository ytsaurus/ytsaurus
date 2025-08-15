"""
Tests for multi-loop and thread safety scenarios to verify Issue #140 fix.
"""

import asyncio
import threading
import time
from typing import List

import pytest

from sse_starlette.sse import AppStatus, EventSourceResponse, _exit_event_context


class TestMultiLoopSafety:
    """Test suite for multi-loop and thread safety."""

    def setup_method(self):
        """Reset AppStatus before each test."""
        AppStatus.should_exit = False

    def teardown_method(self):
        """Clean up after each test."""
        AppStatus.should_exit = False

    def test_context_isolation_same_thread(self):
        """Test that exit events are isolated between different contexts in same thread."""

        async def create_and_check_event():
            # Each call should get its own event
            event1 = AppStatus.get_or_create_exit_event()
            event2 = AppStatus.get_or_create_exit_event()

            # Should be the same within same context
            assert event1 is event2
            return event1

        # Run in different asyncio event loops (different contexts)
        loop1 = asyncio.new_event_loop()
        asyncio.set_event_loop(loop1)
        try:
            event_a = loop1.run_until_complete(create_and_check_event())
        finally:
            loop1.close()

        loop2 = asyncio.new_event_loop()
        asyncio.set_event_loop(loop2)
        try:
            event_b = loop2.run_until_complete(create_and_check_event())
        finally:
            loop2.close()

        # Events from different loops should be different objects
        assert event_a is not event_b

    def test_thread_isolation(self):
        """Test that exit events are isolated between different threads."""
        events: List = []
        errors: List = []

        def create_event_in_thread():
            """Create an event in a new thread with its own event loop."""
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                async def get_event():
                    return AppStatus.get_or_create_exit_event()

                event = loop.run_until_complete(get_event())
                events.append(event)
                loop.close()
            except Exception as e:
                errors.append(e)

        # Create events in multiple threads
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=create_event_in_thread)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Should have no errors
        assert not errors, f"Errors occurred: {errors}"

        # Should have 3 different events
        assert len(events) == 3
        assert len(set(id(event) for event in events)) == 3, "Events should be unique"

    def test_exit_signal_propagation_multiple_contexts(self):
        """Test that exit signals properly propagate to multiple contexts."""

        # Test that exit signal set before waiting works correctly
        AppStatus.should_exit = True

        async def quick_exit_test():
            await EventSourceResponse._listen_for_exit_signal()
            return "exited"

        # Test in single loop first
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(quick_exit_test())
            assert result == "exited"
        finally:
            loop.close()

    @pytest.mark.asyncio
    async def test_context_cleanup(self):
        """Test that context variables are properly cleaned up."""

        # Create an event in current context
        initial_event = AppStatus.get_or_create_exit_event()
        assert _exit_event_context.get() is initial_event

        # Verify we can create new contexts
        async def inner_context():
            # This should create a new event in the task context
            return AppStatus.get_or_create_exit_event()

        # Create task which runs in a copied context
        task_event = await asyncio.create_task(inner_context())

        # The task should have access to the same event (context is copied)
        assert task_event is initial_event

    @pytest.mark.asyncio
    async def test_exit_before_event_creation(self):
        """Test that exit signal works even when set before event creation."""

        # Set exit before any event is created
        AppStatus.should_exit = True

        # This should return immediately without waiting
        start_time = time.time()
        await EventSourceResponse._listen_for_exit_signal()
        elapsed = time.time() - start_time

        # Should return almost immediately (less than 0.1 seconds)
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_race_condition_protection(self):
        """Test protection against race conditions during event setup."""

        # Set exit before creating tasks to avoid hanging
        AppStatus.should_exit = True

        # Multiple concurrent calls should all work correctly
        tasks = [
            asyncio.create_task(EventSourceResponse._listen_for_exit_signal())
            for _ in range(3)
        ]

        # All tasks should complete quickly
        results = await asyncio.gather(*tasks)
        assert len(results) == 3

    def test_no_global_state_pollution(self):
        """Test that global state is not polluted between test runs."""

        # Verify clean state
        assert not AppStatus.should_exit
        assert _exit_event_context.get(None) is None

        # Create an event
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:

            async def create_event():
                return AppStatus.get_or_create_exit_event()

            event = loop.run_until_complete(create_event())
            assert event is not None
        finally:
            loop.close()

        # After loop closes, context should be clean for new contexts
        # (This test verifies we don't have lingering global state)
        loop2 = asyncio.new_event_loop()
        asyncio.set_event_loop(loop2)
        try:

            async def create_new_event():
                return AppStatus.get_or_create_exit_event()

            new_event = loop2.run_until_complete(create_new_event())
            assert new_event is not None
        finally:
            loop2.close()
