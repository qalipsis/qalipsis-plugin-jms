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

import assertk.assertThat
import assertk.assertions.containsOnly
import com.fasterxml.jackson.databind.ObjectMapper
import io.qalipsis.plugins.jms.Constants
import io.qalipsis.runtime.test.QalipsisTestRunner
import org.apache.activemq.ActiveMQConnectionFactory
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import javax.jms.Connection
import javax.jms.DeliveryMode
import javax.jms.Destination
import javax.jms.MessageProducer
import javax.jms.Session
import kotlin.math.pow

/**
 * @author Alexander Sosnovsky
 */
@Testcontainers
internal class JmsScenarioIntegrationTest {

    private lateinit var connectionFactory: ActiveMQConnectionFactory

    private lateinit var producerConnection: Connection

    private lateinit var producerSession: Session

    private var initialized = false

    @BeforeAll
    internal fun setUp() {
        if (!initialized) {
            connectionFactory = ActiveMQConnectionFactory("tcp://localhost:" + container.getMappedPort(61616))
            producerConnection = connectionFactory.createConnection()
            producerConnection.start()

            producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)

            JmsScenario.queueConnection = connectionFactory.createQueueConnection()

            initialized = true
        }
    }

    @AfterAll
    internal fun tearDown() {
        producerSession.close()
        producerConnection.close()
    }


    @Test
    @Timeout(50)
    internal fun `should run the consumer scenario`() {
        val producer1 = prepareQueueProducer("queue-1")
        val producer2 = prepareQueueProducer("queue-2")
        sendMessage(ObjectMapper().writeValueAsString(JmsScenario.User("10", "alex")), producer1)
        sendMessage(ObjectMapper().writeValueAsString(JmsScenario.User("20", "bob")), producer1)
        sendMessage(ObjectMapper().writeValueAsString(JmsScenario.User("10", "charly")), producer2)
        sendMessage(ObjectMapper().writeValueAsString(JmsScenario.User("20", "david")), producer2)

        JmsScenario.receivedMessages.clear()
        val exitCode = QalipsisTestRunner.withScenarios("consumer-jms").execute()
        Assertions.assertEquals(0, exitCode)

        // FIXME, on some cases, receivedMessages is empty here when the scenario is completed,
        // and the expected values are actually visible in the next test.
        assertThat(
            listOf(
                JmsScenario.receivedMessages.poll(),
                JmsScenario.receivedMessages.poll()
            )
        ).containsOnly("10", "20")
    }

    @Test
    @Timeout(50)
    internal fun `should run the consumer scenario with string deserializer`() {
        val producer = prepareQueueProducer("queue-3")
        sendMessage("jms", producer)
        sendMessage("jms2", producer)

        JmsScenario.receivedMessages.clear()
        val exitCode = QalipsisTestRunner.withScenarios("consumer-jms-string-deserializer").execute()

        Assertions.assertEquals(0, exitCode)
        assertThat(
            listOf(
                JmsScenario.receivedMessages.poll(),
                JmsScenario.receivedMessages.poll()
            )
        ).containsOnly("jms", "jms2")
    }

    private fun prepareQueueProducer(queueName: String): MessageProducer {
        val destination: Destination = producerSession.createQueue(queueName)

        val producer = producerSession.createProducer(destination)
        producer.deliveryMode = DeliveryMode.NON_PERSISTENT

        return producer
    }

    /**
     * Use a byte message for testing purpose, because the conversion cannot be unit tested.
     */
    private fun sendMessage(textMessage: String, producer: MessageProducer) {
        val message = producerSession.createBytesMessage()
        message.writeBytes(textMessage.toByteArray())
        producer.send(message)
    }

    companion object {

        @Container
        @JvmStatic
        private val container = GenericContainer<Nothing>(Constants.DOCKER_IMAGE).apply {
            withExposedPorts(61616)
            withCreateContainerCmdModifier {
                it.hostConfig!!.withMemory(256 * 1024.0.pow(2).toLong()).withCpuCount(1)
            }
        }

    }
}
