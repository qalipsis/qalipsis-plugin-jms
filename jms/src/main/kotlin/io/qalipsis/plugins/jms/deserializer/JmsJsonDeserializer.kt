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

package io.qalipsis.plugins.jms.deserializer

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.plugins.jms.JmsDeserializer
import javax.jms.BytesMessage
import javax.jms.Message
import javax.jms.TextMessage
import kotlin.reflect.KClass


class JmsJsonDeserializer<V : Any>(
    private val targetClass: KClass<V>,
    mapperConfiguration: (JsonMapper.() -> Unit)? = null
) : JmsDeserializer<V> {

    private val mapper = JsonMapper()

    init {
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule.Builder().build())
        mapper.registerModule(Jdk8Module())

        mapperConfiguration?.let {
            mapper.mapperConfiguration()
        }
    }

    /**
     * Deserializes the [message] using the jackson Json library to the specified class [V].
     */
    override fun deserialize(message: Message): V {
        when (message) {
            is TextMessage -> return mapper.readValue(message.text, targetClass.java)
            is BytesMessage -> {
                message.reset()
                val byteArray = ByteArray(message.bodyLength.toInt())
                message.readBytes(byteArray)
                return mapper.readValue(byteArray, targetClass.java)
            }
            else -> throw IllegalArgumentException("The message of type ${message::class} is not supported")
        }
    }
}