package main

import com.sksamuel.pulsar4s.{DefaultProducerMessage, EventTime, ProducerConfig, PulsarClient, Topic}

import io.circe.generic.auto._
import com.sksamuel.pulsar4s.circe._
import scala.concurrent.ExecutionContext.Implicits.global

import io.circe.generic.auto._
import com.sksamuel.pulsar4s.circe._
import com.sksamuel.pulsar4s.circe.circeSchema
import com.sksamuel.pulsar4s.{ConsumerConfig, DefaultProducerMessage, EventTime, ProducerConfig, PulsarClient, Subscription, Topic}
import main.PulsarProducer.ec
import main.SensorDomain.SensorEvent
import org.apache.pulsar.client.api.{SubscriptionInitialPosition, SubscriptionType}

import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Success}

object SensorDomain{
  val startupTime = System.currentTimeMillis()
  case class SensorEvent(sensorId: String, status: String, startupTime: Long, eventTime: Long, reading: Double)

  val sensorIds =  (0 to 10).map(_=>UUID.randomUUID().toString).toList
  val offSensors = sensorIds.toSet
  val onSensors = Set[String]()

  def generate(ids: List[String] = sensorIds, off: Set[String] = offSensors, on: Set[String] = onSensors): Iterator[SensorEvent]={
  Thread.sleep(Random.nextInt(500) + 200)

    val index = Random.nextInt(sensorIds.size)
    val sensorId = sensorIds(index)
    val reading = if(off(sensorId)){
      println(s"starting sensor $index")
      SensorEvent(sensorId, "Starting",
        startupTime,
        System.currentTimeMillis(),0.0)
    }else{
      val temp = BigDecimal(40 + Random.nextGaussian()).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble
      SensorEvent(sensorId, "Running", startupTime, System.currentTimeMillis(), temp)
    }
    Iterator.single(reading) ++ generate(ids, off - sensorId, on + sensorId)
  }

  def main(args: Array[String]): Unit = {
    generate().take(100).foreach(println)
  }
}

object PulsarProducer{
  import SensorDomain._

  val executor = Executors.newFixedThreadPool(4)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

  // implicit val asyncHandler: FutureAsyncHandler = new FutureAsyncHandler // <-- generated

  val pulsarClient = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("sensor-events")
  val eventProducer = pulsarClient.producer[SensorEvent](
    ProducerConfig(topic, producerName = Some("sensor-producer"), enableBatching = Some(true), blockIfQueueFull = Some(true))
  )

  def main(args: Array[String]): Unit = {
    SensorDomain.generate().take(100).map{ sensorEvent =>
      val message = DefaultProducerMessage(
        Some(sensorEvent.sensorId),
        sensorEvent,
        eventTime = Some(EventTime(sensorEvent.eventTime))
      )
      eventProducer.sendAsync(message)
    }
  }
}

object PulsarConsumer{

  val pulsarClient = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("sensor-events")

  val consumerConfig = ConsumerConfig(
    Subscription("Sensor-Event-subscription"),
    Seq(topic),
    consumerName = Some("sensor-events-consumer"),
    subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest),
    subscriptionType = Some(SubscriptionType.Exclusive)
  )
  val consumer = pulsarClient.consumer[SensorEvent](consumerConfig)

  def receiveAll(totalMessageCount: Int = 0): Unit = consumer.receive match{
    case Success(message) =>
      println(s"total messagens: $totalMessageCount - Acked message ${message.messageId} - ${message.value}")
      consumer.acknowledge(message.messageId)
      receiveAll(totalMessageCount + 1)
    case Failure(ex) =>
      println(s"failed to receive massage: ${ex.getMessage}")
  }

  def main(args: Array[String]): Unit = {
    receiveAll()
  }
}