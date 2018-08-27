package com.ruimo.jobbroker.queue

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.ruimo.jobbroker.JobId
import org.specs2.mutable.Specification
import com.ruimo.scoins.LoanPattern._

class JobQueueSpec extends Specification {
  "Job queue" should {
    "Can submit job." in {
      val factory = new MockConnectionFactory
      using(factory.newConnection) { conn =>
        using (conn.createChannel) { channel =>
          val jobQueue = new JobQueue(channel)
          jobQueue.channel === channel
          jobQueue.queueName === JobQueue.QueueNameDefault
          jobQueue.exchangeDefault === JobQueue.ExchangeDefault

          jobQueue.submitJob(JobId(123L))
          val resp: Array[Byte] = channel.basicGet(JobQueue.QueueNameDefault, true).getBody
          JobId.fromByteArray(resp).value === 123L
        }.get
      }.get
    }

    "Try to obtain job and canceled." in {
      val factory = new MockConnectionFactory
      @volatile var jobObtainedCalled = false
      @volatile var onCancelCalled = false
      @volatile var onErrorCalled = false

      using(factory.newConnection) { conn =>
        using(conn.createChannel) { channel =>
          val jobQueue = new JobQueue(channel)
          val jobHandle: WaitingJobHandle = jobQueue.waitJob(
            onJobObtained = ary => {jobObtainedCalled = true},
            onCancel = () => {onCancelCalled = true},
            onError = t => {onErrorCalled = true}
          )

          jobQueue.cancelJobWaiting(jobHandle)
          jobObtainedCalled === false
          onCancelCalled === true
          onErrorCalled === false
        }.get
      }.get
    }

    "Try to obtain job and canceled but error." in {
      val factory = new MockConnectionFactory
      @volatile var jobObtainedCalled = false
      @volatile var onCancelCalled = false
      @volatile var error: Option[Throwable] = None
      var t = new Throwable

      using(factory.newConnection) { conn =>
        using(conn.createChannel) { channel =>
          val jobQueue = new JobQueue(channel)
          val jobHandle: WaitingJobHandle = jobQueue.waitJob(
            onJobObtained = ary => {jobObtainedCalled = true},
            onCancel = () => {
              onCancelCalled = true
              throw t
            },
            onError = t => {error = Some(t)}
          )

          jobQueue.cancelJobWaiting(jobHandle)
          jobObtainedCalled === false
          onCancelCalled === true
          error === Some(t)
        }.get
      }.get
    }

    "Can obtain job." in {
      val factory = new MockConnectionFactory
      @volatile var job: Option[JobId] = None
      @volatile var onCancelCalled = false
      @volatile var onErrorCalled = false

      using(factory.newConnection) { conn =>
        using(conn.createChannel) { channel =>
          val jobQueue = new JobQueue(channel)
          val jobHandle: WaitingJobHandle = jobQueue.waitJob(
            onJobObtained = jobId => {job = Some(jobId)},
            onCancel = () => {onCancelCalled = true},
            onError = t => {onErrorCalled = true}
          )

          jobQueue.submitJob(JobId(123L))
          job must be_==(Some(JobId(123L))).eventually
          onCancelCalled === false
          onErrorCalled === false
        }.get
      }.get
    }

    "If error occured while obtaining job, the job is discarded." in {
      val factory = new MockConnectionFactory
      @volatile var job: Option[JobId] = None
      @volatile var onCancelCalled = false
      @volatile var error: Option[Throwable] = None
      @volatile var unexpected = false
      val t = new Throwable

      using(factory.newConnection) { conn =>
        using(conn.createChannel) { channel =>
          val jobQueue = new JobQueue(channel)
          val jobHandle: WaitingJobHandle = jobQueue.waitJob(
            onJobObtained = jobId => {
              if (job == None) {
                job = Some(jobId)
                throw t
              } else {
                unexpected = true
              }
            },
            onCancel = () => {onCancelCalled = true},
            onError = t => {error = Some(t)}
          )

          jobQueue.submitJob(JobId(123L))
          job must be_==(Some(JobId(123L))).eventually
          onCancelCalled === false
          error === Some(t)
          Thread.sleep(100)
          unexpected === false
        }.get
      }.get
    }
  }
}
