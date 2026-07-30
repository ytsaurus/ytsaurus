# Copyright 2026 Confluent Inc.
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

"""Marker constants identifying the AWS IAM OAUTHBEARER autowire path.

The config-key/value pair that activates the AWS OAUTHBEARER autowire path.

The C dispatcher in ``src/confluent_kafka/src/confluent_kafka.c`` keeps its
own literal copies of these values for compile-time use; the drift-guard
test in ``tests/oauthbearer/aws/test_aws_iam_marker.py`` asserts the C-side
literals and these Python constants stay in lock-step.

These strings are part of the cross-language wire contract — bumping either
is a major version change on ``confluent-kafka``.
"""

__all__ = ["AWS_IAM_MARKER_KEY", "AWS_IAM_MARKER_VALUE"]


#: Config key that activates the AWS IAM autowire path when set
#: to :data:`AWS_IAM_MARKER_VALUE`.
AWS_IAM_MARKER_KEY: str = "sasl.oauthbearer.metadata.authentication.type"

#: On-wire value of :data:`AWS_IAM_MARKER_KEY` that selects AWS IAM
#: authentication.
AWS_IAM_MARKER_VALUE: str = "aws_iam"
