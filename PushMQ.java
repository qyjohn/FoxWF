import java.io.*;
import com.rabbitmq.client.*;

public class PushMQ
{
	Connection connection;
	Channel channel;
	QueueingConsumer consumer;
	String mq_name;

	public PushMQ(String master, String name)
	{
		mq_name = name;
	  try
	  {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(master);
			connection = factory.newConnection();
			channel = connection.createChannel();	  		  
			channel.queueDeclare(mq_name, false, false, false, null);
	  } catch (Exception e)
	  {
		  System.out.println(e.getMessage());
		  e.printStackTrace();
	  }
		
	}

	public void pushMQ(String msg)
	{		
		try
		{
			channel.basicPublish("", mq_name, null, msg.getBytes());
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args)
	{
		String msg;
		PushMQ pj = new PushMQ("localhost", FoxParam.SIMPLE_WORKFLOW_JOB_MQ);
		
		while (true)
		{
			pj.pushMQ("<job project='1d106dda-fc5a-4d60-bae6-5d12fcb703b9' id='ID000970' path='/data/Test_6.0_01' />");
		}
	}
}