

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class Consumer extends Thread
{
  private final ConsumerConnector consumer;
  private final String topic;
  
  public Consumer(String topic)
  {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig());
    this.topic = topic;
  }

  private static ConsumerConfig createConsumerConfig()
  {
    Properties props = new Properties();
    props.put("zookeeper.connect", KafkaProperties.zkConnect);
    props.put("group.id", KafkaProperties.groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);

  }
  public static long bytesToLong(byte[] b) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    DataOutputStream out;
    long filesize;
    while(it.hasNext()) {
      try {
        String byteString =new String(it.next().message(), "UTF-8");
        out =new DataOutputStream(new FileOutputStream(byteString));
        filesize=bytesToLong(it.next().message());
        while(it.hasNext())
        {
          byte[] messageReturned=it.next().message();
          out.write(messageReturned);
          filesize=filesize-messageReturned.length;
          if(filesize<=0) break;
        }
        new ProcessBuilder("curl", "http://localhost:8983/solr/new_core/update",
                "-H", "\"Content-Type: text/xml\"", "--data-binary", "@"+byteString).start();
        out.flush();
        out.close();
        new File(byteString).deleteOnExit();
    } catch (Exception e) {
        e.printStackTrace();
      }
      //System.out.println(new String(it.next().message()));
    }

  }
}
