package io.qalipsis.plugins.jms.producer

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.plugins.jms.jms
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.command.ActiveMQTopic
import org.junit.jupiter.api.Test

/**
 *
 * @author Alexander Sosnovsky
 * */
internal class JmsProducerStepSpecificationTest {

    private val connectionFactory = ActiveMQConnectionFactory().createConnection()

    @Test
    internal fun `should apply connection`() {
        val rec1 = JmsProducerRecord(
            destination = ActiveMQTopic().createDestination("dest-1"),
            value = "text-1"
        )
        val rec2 = JmsProducerRecord(
            destination = ActiveMQTopic().createDestination("dest-2"),
            value = "text-2"
        )

        val recordSupplier: (suspend (ctx: StepContext<*, *>, input: Any?) -> List<JmsProducerRecord>) = { _, _ -> listOf(rec1, rec2) }

        val previousStep = DummyStepSpecification()
        previousStep.jms().produce {
            name = "my-producer-step"
            connect { connectionFactory }
            records(recordSupplier)
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(JmsProducerStepSpecificationImpl::class).all {
            prop("name") { JmsProducerStepSpecificationImpl<*>::name.call(it) }.isEqualTo("my-producer-step")

            prop(JmsProducerStepSpecificationImpl<*>::recordsFactory).isEqualTo(recordSupplier)

            prop(JmsProducerStepSpecificationImpl<*>::metrics).isNotNull().all {
                prop(JmsProducerMetricsConfiguration::bytesCount).isFalse()
                prop(JmsProducerMetricsConfiguration::recordsCount).isFalse()
            }
        }

        val step = previousStep.nextSteps[0] as JmsProducerStepSpecification<*>
        val connectFactory = step.getProperty<() -> String>("connectionFactory")
        assertThat(connectFactory.invoke()).isEqualTo(connectionFactory)
    }


    @Test
    internal fun `should apply bytes count`() {
        val recordSuplier: (suspend (ctx: StepContext<*, *>, input: Any?) -> List<JmsProducerRecord>) = { _, _ -> listOf() }

        val scenario = DummyStepSpecification()
        scenario.jms().produce {
            connect { connectionFactory }
            records(recordSuplier)

            metrics {
                bytesCount = true
            }
        }

        assertThat(scenario.nextSteps[0]).isInstanceOf(JmsProducerStepSpecificationImpl::class).all {
            prop(JmsProducerStepSpecificationImpl<*>::metrics).isNotNull().all {
                prop(JmsProducerMetricsConfiguration::bytesCount).isTrue()
                prop(JmsProducerMetricsConfiguration::recordsCount).isFalse()
            }
        }
    }

    @Test
    internal fun `should apply records count`() {
        val recordSuplier: (suspend (ctx: StepContext<*, *>, input: Any?) -> List<JmsProducerRecord>) = { _, _ -> listOf() }

        val scenario = DummyStepSpecification()
        scenario.jms().produce {
            connect { connectionFactory }
            records(recordSuplier)

            metrics {
                recordsCount = true
            }
        }

        assertThat(scenario.nextSteps[0]).isInstanceOf(JmsProducerStepSpecificationImpl::class).all {
            prop(JmsProducerStepSpecificationImpl<*>::metrics).isNotNull().all {
                prop(JmsProducerMetricsConfiguration::bytesCount).isFalse()
                prop(JmsProducerMetricsConfiguration::recordsCount).isTrue()
            }
        }
    }

}
