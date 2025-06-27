/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.jms

import javax.jms.Message


/**
 * Deserializer from a JMS [Message] to a user-defined type.
 */
interface JmsDeserializer<V> {

    /**
     * Deserializes the [message] to the specified type [V].
     *
     * @param message consumed from JMS.
     * @return [V] the specified type to return after deserialization.
     */
    fun deserialize(message: Message): V
}
