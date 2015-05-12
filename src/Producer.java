
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread
{
  private final kafka.javaapi.producer.Producer<Integer, Object> producer;
  private final String topic;
  private final Properties props = new Properties();

  public Producer(String topic)
  {
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, Object>(new ProducerConfig(props));
    this.topic = topic;
  }
  
  public void run() {
    int messageNo = 1;
   // while(true)
    {
      //String messageStr = new String("Message_" + messageNo);
      try {

        byte buffer[]=new byte[1024];
        File file =new File("src/file.txt");
        DataInputStream messageStr = new DataInputStream(new FileInputStream(file));
        int n;
        while(( n=messageStr.read(buffer))!=-1) {
          byte[] send = new byte[n];
          for(int i=0;i<n;i++)
          {
            send[i]=buffer[i];
          }
         // System.out.println(new String(send));
          producer.send(new KeyedMessage<Integer, Object>(topic, send));

        }
        messageNo++;
      }
      catch(Exception e)
      {
          System.out.println(e);
      }
    }
  }

}
