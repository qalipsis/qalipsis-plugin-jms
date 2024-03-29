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

package io.qalipsis.plugins.jms.consumer

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.jms.JmsDeserializer
import java.util.concurrent.atomic.AtomicLong
import javax.jms.BytesMessage
import javax.jms.Message
import javax.jms.TextMessage

/**
 * Implementation of [DatasourceObjectConverter], that reads a message from JMS and forwards
 * it converted to as [JmsConsumerRecord].
 *
 * @author Alexander Sosnovsky
 */
internal class JmsConsumerConverter<O : Any?>(
    private val valueDeserializer: JmsDeserializer<O>,
    private val meterRegistry: CampaignMeterRegistry?,
    private val eventsLogger: EventsLogger?
) : DatasourceObjectConverter<Message, JmsConsumerResult<O>> {

    private val eventPrefix: String = "jms.consume"
    private val meterPrefix: String = "jms-consume"
    private var consumedBytesCounter: Counter? = null
    private var consumedRecordsCounter: Counter? = null

    private lateinit var eventTags: Map<String, String>

    override fun start(context: StepStartStopContext) {
        eventTags = context.toEventTags()
        meterRegistry?.apply {
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            consumedBytesCounter =
                counter(scenarioName, stepName, "$meterPrefix-value-bytes", context.toMetersTags()).report {
                display(
                    format = "received: %,.0f bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 0,
                    Counter::count
                )
            }
            consumedRecordsCounter =
                counter(scenarioName, stepName, "$meterPrefix-records", context.toMetersTags()).report {
                display(
                    format = "received rec: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 1,
                    column = 1,
                    Counter::count
                )
            }
        }
    }

    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            consumedBytesCounter = null
            consumedRecordsCounter = null
        }
    }

    override suspend fun supply(
        offset: AtomicLong, value: Message,
        output: StepOutput<JmsConsumerResult<O>>
    ) {
        val jmsConsumerMeters = JmsConsumerMeters()
        eventsLogger?.info("${eventPrefix}.received.records", 1, tags = eventTags)
        consumedRecordsCounter?.increment()

        val bytesCount = when (value) {
            is TextMessage -> value.text.toByteArray().size

            is BytesMessage -> value.bodyLength.toInt()

            else -> 0
        }

        consumedBytesCounter?.increment(bytesCount.toDouble())
        jmsConsumerMeters.consumedBytes = bytesCount

        eventsLogger?.info("${eventPrefix}.received.value-bytes", bytesCount, tags = eventTags)
        output.send(
            JmsConsumerResult(
                JmsConsumerRecord(
                    offset.getAndIncrement(),
                    value,
                    valueDeserializer.deserialize(value)
                ),
                jmsConsumerMeters
            )
        )
    }
}
