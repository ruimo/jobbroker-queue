package com.ruimo.jobbroker.queue

import com.ruimo.jobbroker.JobId
import com.rabbitmq.client.{AMQP, Channel, DefaultConsumer, Envelope}

case class WaitingJobHandle(tag: String) extends AnyVal

object JobQueue {
  val QueueNameDefault = "com.ruimo.jobbroker.jobqueue"
  val ExchangeDefault = "" // Equivalent to "direct"
}

class JobQueue(
  private[queue] val channel: Channel,
  val queueName: String = JobQueue.QueueNameDefault,
  val exchangeDefault: String = JobQueue.ExchangeDefault
) {
  def submitJob(jobId: JobId): Unit = {
    channel.queueDeclare(
      queueName, /* durable = */true, /* exclusive = */false, /* autoDelete = */false,
      /* arguments = */ java.util.Collections.emptyMap[String, AnyRef]()
    )
    channel.basicPublish(
      exchangeDefault,
      queueName,
      /* properties = */null,
      jobId.toByteArray
    )
  }

  def waitJob(
    onJobObtained: JobId => Unit,
    onCancel: () => Unit,
    onError: Throwable => Unit
  ): WaitingJobHandle = {
    channel.queueDeclare(
      queueName, /* durable = */ true, /* exclusive = */ false, /* autoDelete = */ false,
      /* arguments = */ java.util.Collections.emptyMap[String, AnyRef]()
    )
    WaitingJobHandle(
      channel.basicConsume(queueName, /* autoAck*/ false, new DefaultConsumer(channel) {
        override def handleDelivery(
          consumerTag: String,
          envelope: Envelope,
          properties: AMQP.BasicProperties,
          body: Array[Byte]
        ) {
          val deliveryTag: Long = envelope.getDeliveryTag
          try {
            onJobObtained(JobId.fromByteArray(body))
            channel.basicAck(deliveryTag, false /* multiple */)
          } catch {
            case t: Throwable =>
              try {
                channel.basicNack(
                  deliveryTag,
                  false /* multiple */ ,
                  false /* requeue */
                )
                onError(t)
              } catch {
                case t2: Throwable =>
                  t2.addSuppressed(t)
                  onError(t2)
              }
          }
        }

        override def handleCancelOk(consumerTag: String) {
          try {
            onCancel()
          } catch {
            case t: Throwable => onError(t)
          }
        }
      })
    )
  }

  def cancelJobWaiting(
    handle: WaitingJobHandle
  ) {
    channel.basicCancel(handle.tag)
  }
}

