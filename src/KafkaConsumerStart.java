/**
 *
 * Class: Start consumer threads to read and update into solr
 *
 * Created by dsouza on 16-05-2015.
 */
public class KafkaConsumerStart implements KafkaProperties {

  public static void main(String args[]){

    Consumer consumerThread1 = new Consumer(KafkaProperties.topic);
    consumerThread1.start();

    Consumer consumerThread2 = new Consumer(KafkaProperties.topic2);
    consumerThread2.start();

  }
}


