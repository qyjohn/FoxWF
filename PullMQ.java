import java.io.*;
import com.rabbitmq.client.*;

public class PullMQ
{
	Connection connection;
	Channel channel;
	QueueingConsumer consumer;
	String mq_name;

	public PullMQ(String master, String name)
	{
		mq_name = name;
	  try
	  {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(master);
			connection = factory.newConnection();
			channel = connection.createChannel();	  		  
			channel.queueDeclare(mq_name, false, false, false, null);
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(mq_name, true, consumer);				  
	  } catch (Exception e)
	  {
		  System.out.println(e.getMessage());
		  e.printStackTrace();
	  }
		
	}

	public String pullMQ()
	{
		String msg = "";
		
		try
		{
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			msg = new String(delivery.getBody());
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		return msg;
	}
	
	
	public static void main(String[] args)
	{
		String msg;
		PullMQ pj = new PullMQ("localhost", FoxParam.SIMPLE_WORKFLOW_JOB_MQ);
		
		while (true)
		{
			System.out.println(pj.pullMQ());
		}
	}
}