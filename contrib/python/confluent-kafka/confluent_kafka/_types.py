#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Common type definitions for confluent_kafka package.

This module provides centralized type aliases to maintain DRY principle
and ensure consistency across the package.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# Headers can be either dict format or list of tuples format
HeadersType = Union[Dict[str, Union[str, bytes, None]], List[Tuple[str, Union[str, bytes, None]]]]

# Serializer/Deserializer callback types (will need SerializationContext import where used)
Serializer = Callable[[Any, Any], bytes]  # (obj, SerializationContext) -> bytes
Deserializer = Callable[[Optional[bytes], Any], Any]  # (Optional[bytes], SerializationContext) -> obj

# Forward declarations for callback types that reference classes from cimpl
# These are defined here to avoid circular imports
DeliveryCallback = Callable[[Optional[Any], Any], None]  # (KafkaError, Message) -> None
RebalanceCallback = Callable[[Any, List[Any]], None]  # (Consumer, List[TopicPartition]) -> None
AcknowledgementCommitCallback = Callable[[Dict[Any, Any], Optional[Any]], None]  # (offsets, KafkaException) -> None
