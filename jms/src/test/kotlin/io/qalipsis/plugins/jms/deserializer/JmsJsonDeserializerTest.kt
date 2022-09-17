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

package io.qalipsis.plugins.jms

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.prop
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.qalipsis.plugins.jms.deserializer.JmsJsonDeserializer
import org.apache.activemq.command.ActiveMQTextMessage
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import javax.jms.Message

/**
 * @author Alexander Sosnovsky
 */
internal class JmsJsonDeserializerTest {

    internal data class UserExample(val id: Int, val name: String, val createdAt: Instant)

    companion object {
        private val mapper = JsonMapper().registerModule(JavaTimeModule()).registerModule(KotlinModule())
    }

    @Test
    fun `should deserialize json`() {
        val deserializer = JmsJsonDeserializer(UserExample::class)

        val user = UserExample(1, "mike", Instant.now())
        val jsonObject = mapper.writeValueAsString(user)

        val result = deserializer.deserialize(createMessage(jsonObject))

        assertThat(result).all {
            prop(UserExample::name).isEqualTo("mike")
            prop(UserExample::id).isEqualTo(1)
            prop(UserExample::createdAt).isEqualTo(user.createdAt)
        }
    }

    @Test
    fun `should fail when deserializing json with the default object mapper`() {
        val deserializer = JmsJsonDeserializer(UserExample::class)

        val instantString = "2021-03-04T12:17:47.720Z"
        val userString = """{"id":1, "name":"bob", "createdAt":"$instantString", "email":"bob@mail.com"}"""
            .trimMargin()

        assertThrows<JsonMappingException> {
            deserializer.deserialize(createMessage(userString))
        }
    }

    @Test
    fun `should deserialize json with custom mapper configuration`() {
        val deserializer = JmsJsonDeserializer(UserExample::class) {
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        }

        val instantString = "2021-03-04T12:17:47.720Z"
        val userString = """{"id":2, "name":"bob", "createdAt":"$instantString", "email":"bob@mail.com"}"""
            .trimMargin()

        val result = deserializer.deserialize(createMessage(userString))

        assertThat(result).all {
            prop(UserExample::name).isEqualTo("bob")
            prop(UserExample::id).isEqualTo(2)
            prop(UserExample::createdAt).isEqualTo(Instant.parse(instantString))
        }
    }

    private fun createMessage(messageText: String): Message {
        val message = ActiveMQTextMessage()

        message.text = messageText

        return message
    }

}
