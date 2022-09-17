/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.jms.producer

/**
 * Records the metrics for the JMS producer.
 *
 * @property recordsToProduce counts the number of records to be sent.
 * @property producedBytes records the number of bytes sent.
 * @property producedRecords counts the number of records actually sent.
 *
 * @author Alex Averyanov
 */
data class JmsProducerMeters(
    var recordsToProduce: Int = 0,
    var producedBytes: Int = 0,
    var producedRecords: Int = 0
)