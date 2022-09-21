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

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.executionprofile.regular
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.blackHole
import io.qalipsis.api.steps.filterNotNull
import io.qalipsis.api.steps.innerJoin
import io.qalipsis.api.steps.map
import io.qalipsis.api.steps.onEach
import io.qalipsis.plugins.jms.deserializer.JmsJsonDeserializer
import io.qalipsis.plugins.jms.deserializer.JmsStringDeserializer
import io.qalipsis.plugins.jms.jms
import org.apache.activemq.ActiveMQConnectionFactory
import java.beans.ConstructorProperties
import java.util.concurrent.LinkedBlockingDeque

internal object JmsScenario {

    internal var queueConnection = ActiveMQConnectionFactory().createQueueConnection()

    internal const val minions = 2

    internal val receivedMessages = LinkedBlockingDeque<String>(10)

    @Scenario("consumer-jms")
    fun consumeRecords() {
        scenario {
            minionsCount = minions
            profile {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }.start().jms()
            .consume {
                queues("queue-1")
                queueConnection { queueConnection }
            }.deserialize(JmsJsonDeserializer(User::class))
            .innerJoin(
                using = { it.value.record.value.id },
                on = {
                    it.jms()
                        .consume {
                            queues("queue-2")
                            queueConnection { queueConnection }
                        }.deserialize(JmsJsonDeserializer(User::class))
                },
                having = { it.value.record.value.id }
            )
            .filterNotNull()
            .map { joinResult -> joinResult.second.record }
            .onEach {
                receivedMessages.add(it.value.id)
            }
            .blackHole()
    }

    @Scenario("consumer-jms-string-deserializer")
    fun consumeRecordsStringDeserializer() {
        scenario {
            minionsCount = minions
            profile {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }.start().jms()
            .consume {
                queues("queue-3")
                queueConnection { queueConnection }
            }.deserialize(JmsStringDeserializer::class)
            .onEach {
                receivedMessages.add(it.record.value)
            }
            .blackHole()
    }

    data class User @ConstructorProperties("id", "name") constructor(val id: String, val name: String)
}
