
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread
{
  private final kafka.javaapi.producer.Producer<Integer, Object> producer;
  private final String topic;
  private final Properties props = new Properties();
  String path;
  public Producer(String topic,String path)
  {
    this.path=path;
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, Object>(new ProducerConfig(props));
    this.topic = topic;
  }
  public String[] listFiles()
  {
    File folder = new File(path);
    File[] listOfFiles = folder.listFiles();
    int i=0;
    String Files[]= new String[listOfFiles.length];
    while (i<listOfFiles.length)
    {
        System.out.println(listOfFiles[i].getName());
        if(listOfFiles[i].isFile())
        {
          String filename=listOfFiles[i].getName();
          if(filename.endsWith(".xml")) {
            Files[i] = path + "\\" + listOfFiles[i].getName();
          }
        }
        i++;
    }
    return Files;
  }

  public static byte[] longToBytes(long l) {
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte)(l & 0xFF);
      l >>= 8;
    }
    return result;
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
      String[] listOfFiles=listFiles();
      //String messageStr = new String("Message_" + messageNo);
    try {


      byte buffer[]=new byte[2048];
      File file;
      DataInputStream messageStr;
      int n;
      System.out.print(listOfFiles.length);
      for(int i=0;i<listOfFiles.length;i++) {
        file = new File(listOfFiles[i]);
        producer.send(new KeyedMessage<Integer, Object>(topic, file.getName().getBytes()));
        producer.send(new KeyedMessage<Integer, Object>(topic, longToBytes(file.length())));
        messageStr = new DataInputStream(new FileInputStream(file));
        while ((n = messageStr.read(buffer)) != -1) {
          byte[] send = new byte[n];
          for (int j = 0; j < n; j++) {
            send[j] = buffer[j];
          }
          // System.out.println(new String(send));
          producer.send(new KeyedMessage<Integer, Object>(topic, send));
        }
        int f=0;
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }

  }
}
