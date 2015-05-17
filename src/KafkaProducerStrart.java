/**
 *
 * Creates and starts Kafka Producer
 * Created by dsouza on 16-05-2015.
 */
public class KafkaProducerStrart {
  public static void main(String[] args) {
    Producer producerThread1 = new Producer(KafkaProperties.topic, "C:\\kafka\\kafkatemp\\kafka\\topic1");
    producerThread1.start();

    Producer producerThread2 = new Producer(KafkaProperties.topic2, "C:\\kafka\\kafkatemp\\kafka\\topic2");
    producerThread2.start();

  }
}
