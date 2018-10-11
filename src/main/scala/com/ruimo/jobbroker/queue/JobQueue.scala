package com.ruimo.jobbroker.queue

import com.ruimo.jobbroker.JobId
import com.rabbitmq.client.{AMQP, Channel, DefaultConsumer, Envelope}
import org.slf4j.{Logger, LoggerFactory}

case class WaitingJobHandle(tag: String) extends AnyVal

object JobQueue {
  val QueueNameDefault = "com.ruimo.jobbroker.jobqueue"
  val ExchangeDefault = "" // Equivalent to "direct"
  val Logger: Logger = LoggerFactory.getLogger(JobQueue.getClass)
}

class JobQueue(
  private[queue] val channel: Channel,
  val queueName: String = JobQueue.QueueNameDefault,
  val exchangeDefault: String = JobQueue.ExchangeDefault
) {
  def submitJob(jobId: JobId): Unit = {
    JobQueue.Logger.info("declaring queue: '" + queueName + "'")
    channel.queueDeclare(
      queueName, /* durable = */true, /* exclusive = */false, /* autoDelete = */false,
      /* arguments = */ java.util.Collections.emptyMap[String, AnyRef]()
    )
    JobQueue.Logger.info("publishing message(id = " + jobId + ") to queue.'")
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
    onError: (JobId, Throwable) => Unit
  ): WaitingJobHandle = {
    def notifyError(jobId: JobId, t: Throwable): Unit = try {
      onError(jobId, t)
    } catch {
      case t0: Throwable => JobQueue.Logger.error("Unexpected error in onError.", t0)
    }

    JobQueue.Logger.info("Declaring queue '" + queueName + "'")
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
          JobQueue.Logger.info("handleDelivery called. tag: '" + consumerTag + "'")

          val deliveryTag: Long = envelope.getDeliveryTag
          val jobId = JobId.fromByteArray(body)
          try {
            onJobObtained(jobId)
            channel.basicAck(deliveryTag, false /* multiple */)
          } catch {
            case t: Throwable =>
              try {
                channel.basicNack(
                  deliveryTag,
                  false /* multiple */ ,
                  false /* requeue */
                )

                notifyError(jobId, t)
              } catch {
                case t2: Throwable =>
                  t2.addSuppressed(t)
                  notifyError(jobId, t2)
              }
          }
        }

        override def handleCancelOk(consumerTag: String) {
          JobQueue.Logger.info("handleCancelOk called. tag: '" + consumerTag + "'")
          try {
            onCancel()
          } catch {
            case t: Throwable =>
              JobQueue.Logger.error("Error while canceling worker.", t)
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
