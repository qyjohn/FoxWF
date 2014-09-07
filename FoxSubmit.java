import java.io.*;
import java.util.Properties;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class FoxSubmit
{
	public static void main(String[] args)
	{

		try
		{
			// Compose an MQ message representing the project
			String name = args[0];
			String path = args[1];
			String msg = "<project name='" + name +"' path='" + path + "'/>";

			// Load configuration from config.properties
			Properties prop = new Properties();
			String propFileName = "config.properties";
			InputStream inputStream = new FileInputStream(propFileName);
			prop.load(inputStream);

			// Get the property value and print it out
			String master = prop.getProperty("master");
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(master);
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
    			
			channel.queueDeclare(FoxParam.SIMPLE_WORKFLOW_PRJ_MQ, false, false, false, null);
			channel.basicPublish("", FoxParam.SIMPLE_WORKFLOW_PRJ_MQ, null, msg.getBytes());
			
			channel.close();
			connection.close();		
				
			// Debug
			System.out.println("Workflow submitted to master " + master + ".");
			
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
}