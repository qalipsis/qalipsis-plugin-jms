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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.prop
import io.mockk.coEvery
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.plugins.jms.JmsDeserializer
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.CleanMockkRecordedCalls
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import org.apache.activemq.command.ActiveMQDestination
import org.apache.activemq.command.ActiveMQTextMessage
import org.apache.activemq.command.ActiveMQTopic
import org.apache.activemq.command.MessageId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.atomic.AtomicLong
import javax.jms.Message
import javax.jms.TextMessage


/**
 *
 * @author Alexander Sosnovsky
 */
@CleanMockkRecordedCalls
internal class JmsConsumerConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val valueDeserializer: JmsDeserializer<String> = relaxedMockk {
        every { deserialize(any()) } answers { firstArg<TextMessage>().text }
    }

    private val metersTags: Map<String, String> = mockk<Map<String, String>>()

    private val eventsTags: Map<String, String> = mockk<Map<String, String>>()

    private val startStopContext = relaxedMockk<StepStartStopContext> {
        every { toMetersTags() } returns metersTags
        every { toEventTags() } returns eventsTags
        every { scenarioName } returns "scenario-name"
        every { stepName } returns "step-name"
    }

    private val consumedBytesCounter = relaxedMockk<Counter>()

    private val consumedRecordsCounter = relaxedMockk<Counter>()

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
        every {
            counter(
                "scenario-name",
                "step-name",
                "jms-consume-value-bytes",
                refEq(metersTags)
            )
        } returns consumedBytesCounter
        every { consumedBytesCounter.report(any()) } returns consumedBytesCounter
        every {
            counter(
                "scenario-name",
                "step-name",
                "jms-consume-records",
                refEq(metersTags)
            )
        } returns consumedRecordsCounter
        every { consumedRecordsCounter.report(any()) } returns consumedRecordsCounter
    }

    @Timeout(2)
    @Test
    internal fun `should deserialize without monitor`() = testDispatcherProvider.runTest {
        // when
        executeConversion()
        val consumedBytesCounter = relaxedMockk<Counter>()
        confirmVerified(consumedBytesCounter)
    }

    @Timeout(2)
    @Test
    internal fun `should deserialize and monitor`() = testDispatcherProvider.runTest {
        // when
        executeConversion(meterRegistry = meterRegistry, eventsLogger = eventsLogger)

        verify {
            consumedBytesCounter.increment(14.0)
            consumedRecordsCounter.increment()
            consumedBytesCounter.increment(14.0)
            consumedRecordsCounter.increment()
            consumedBytesCounter.increment(14.0)
            consumedRecordsCounter.increment()
            consumedBytesCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            consumedRecordsCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
        }

        verifyOrder {
            eventsLogger.info(
                "jms.consume.received.records",
                1,
                timestamp = any(),
                tags = refEq(eventsTags)
            )
            eventsLogger.info(
                "jms.consume.received.value-bytes",
                14,
                timestamp = any(),
                tags = refEq(eventsTags)
            )
            eventsLogger.info(
                "jms.consume.received.records",
                1,
                timestamp = any(),
                tags = refEq(eventsTags)
            )
            eventsLogger.info(
                "jms.consume.received.value-bytes",
                14,
                timestamp = any(),
                tags = refEq(eventsTags)
            )
            eventsLogger.info(
                "jms.consume.received.records",
                1,
                timestamp = any(),
                tags = refEq(eventsTags)
            )
            eventsLogger.info(
                "jms.consume.received.value-bytes",
                14,
                timestamp = any(),
                tags = refEq(eventsTags)
            )
        }
        confirmVerified(consumedBytesCounter, eventsLogger)
    }

    private suspend fun executeConversion(
        meterRegistry: CampaignMeterRegistry? = null,
        eventsLogger: EventsLogger? = null
    ) {

        val converter = JmsConsumerConverter(
            valueDeserializer, meterRegistry, eventsLogger
        )

        val offset = AtomicLong(1)

        val topic1 = ActiveMQTopic("topic-1")
        val destination1 = topic1.createDestination("dest-1")

        val topic2 = ActiveMQTopic("topic-2")
        val destination2 = topic2.createDestination("dest-2")

        val message1 = generateMessage("test-message-1", destination1, 1)
        val message2 = generateMessage("test-message-2", destination1, 2)
        val message3 = generateMessage("test-message-3", destination2, 3)
        message3.jmsPriority = 33
        val channel = Channel<JmsConsumerResult<String>>(3)
        val output = relaxedMockk<StepOutput<JmsConsumerResult<String>>> {
            coEvery { send(any()) } coAnswers {
                channel.send(firstArg())
            }
        }

        //when
        converter.start(startStopContext)
        converter.supply(
            offset, message1, output
        )
        converter.supply(
            offset, message2, output
        )
        converter.supply(
            offset, message3, output
        )
        // Each message is sent in a unitary statement.
        val results = listOf(
            channel.receive(),
            channel.receive(),
            channel.receive()
        )

        // then
        assertThat(results).all {
            hasSize(3)
            index(0).all {
                prop(JmsConsumerResult<*>::record).all {
                    prop(JmsConsumerRecord<*>::timestamp).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::expiration).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::priority).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::correlationId).isEqualTo("correlation-id-1")
                    prop(JmsConsumerRecord<*>::messageId).isEqualTo("key-id-1")
                    prop(JmsConsumerRecord<*>::offset).isEqualTo(1)
                    prop(JmsConsumerRecord<*>::destination).isEqualTo(destination1)
                    prop(JmsConsumerRecord<*>::value).isEqualTo("test-message-1")
                }
                prop(JmsConsumerResult<*>::meters).all {
                    prop(JmsConsumerMeters::consumedBytes).isEqualTo(14)
                }
            }

            index(1).all {
                prop(JmsConsumerResult<*>::record).all {
                    prop(JmsConsumerRecord<*>::timestamp).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::expiration).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::priority).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::correlationId).isEqualTo("correlation-id-2")
                    prop(JmsConsumerRecord<*>::messageId).isEqualTo("key-id-2")
                    prop(JmsConsumerRecord<*>::offset).isEqualTo(2)
                    prop(JmsConsumerRecord<*>::destination).isEqualTo(destination1)
                    prop(JmsConsumerRecord<*>::value).isEqualTo("test-message-2")
                }
                prop(JmsConsumerResult<*>::meters).all {
                    prop(JmsConsumerMeters::consumedBytes).isEqualTo(14)
                }
            }

            index(2).all {
                prop(JmsConsumerResult<*>::record).all {
                    prop(JmsConsumerRecord<*>::timestamp).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::expiration).isEqualTo(0)
                    prop(JmsConsumerRecord<*>::priority).isEqualTo(33)
                    prop(JmsConsumerRecord<*>::correlationId).isEqualTo("correlation-id-3")
                    prop(JmsConsumerRecord<*>::messageId).isEqualTo("key-id-3")
                    prop(JmsConsumerRecord<*>::offset).isEqualTo(3)
                    prop(JmsConsumerRecord<*>::destination).isEqualTo(destination2)
                    prop(JmsConsumerRecord<*>::value).isEqualTo("test-message-3")
                }
                prop(JmsConsumerResult<*>::meters).all {
                    prop(JmsConsumerMeters::consumedBytes).isEqualTo(14)
                }
            }
        }

    }

    private fun generateMessage(
        text: String,
        destination: ActiveMQDestination,
        offset: Long
    ): Message {
        val message = ActiveMQTextMessage()
        val msgId = MessageId("key-id-$offset")

        message.text = text
        message.destination = destination
        message.messageId = msgId
        message.correlationId = "correlation-id-$offset"

        return message
    }

}
