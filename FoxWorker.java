import java.io.*;
import org.dom4j.*;
import java.net.Inet4Address;
import java.lang.management.*;
import java.util.HashSet;
import java.util.Properties;

public class FoxWorker
{
	PushMQ amq;	// ACK MQ
	PullMQ jmq;	// JOB MQ

	HashSet<String> runningJobs;
	String server, worker;
	int max_cpu, max_thread;
	
	public FoxWorker(String s, int cpu, int thread)
	{
		try
		{
			server = s;
			worker = Inet4Address.getLocalHost().getHostAddress();
			
			jmq = new PullMQ(server, FoxParam.SIMPLE_WORKFLOW_JOB_MQ);
			amq = new PushMQ(server, FoxParam.SIMPLE_WORKFLOW_ACK_MQ);
	
			max_cpu = cpu;
			max_thread = thread;
			runningJobs = new HashSet<String>();		
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public String getTask()
	{
		String msg = jmq.pullMQ();
		
		return msg;
	}
	
	public synchronized void execTask(String msg)
	{
		try
		{
			Element job = DocumentHelper.parseText(msg).getRootElement();
			String project = job.attribute("project").getValue();
			String path = job.attribute("path").getValue();
			String jobId = job.attribute("id").getValue();
			String command = job.element("command").getText().trim();
	
			// Add the current running task to local runningJobs HashSet
			runningJobs.add(jobId);
			// Start a new thread to run the task
			new WorkerThread(amq, worker, runningJobs, max_cpu, project, path, jobId, command).start();	
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args)
	{
		try
		{
			// Load configuration from config.properties
			Properties prop = new Properties();
			String propFileName = "config.properties";
			InputStream inputStream = new FileInputStream(propFileName);
			prop.load(inputStream);

			// Get the property value and print it out
			String master = prop.getProperty("master");
			int cpu_factor = Integer.parseInt(prop.getProperty("cpu_factor"));
			String secret = prop.getProperty("secret");
			
			// Check if the command line supplies a master address
			if (args.length == 1)
			{
				master = args[0];	// The first argument should be the IP or hostname of the master	
			}
			
			
			// Determine the maximum number of task threads
			OperatingSystemMXBean mb = ManagementFactory.getOperatingSystemMXBean();
			int cpu = mb.getAvailableProcessors();
			int max_thread = cpu_factor * cpu;
			
			// Create a worker node
			FoxWorker worker = new FoxWorker(master, cpu, max_thread);
			while (true)
			{
				try
				{
					while (worker.runningJobs.size() >= max_thread)
					{
						Thread.sleep(10);
					}					
					// Get next task and execute
//					worker.execTask(worker.getTask());
					worker.execTask(worker.getTask());
					
				} catch (Exception e1)
				{
					System.out.println(e1.getMessage());
					e1.printStackTrace();					
				}
			}			
		} catch (Exception e2)
		{
			System.out.println(e2.getMessage());
			e2.printStackTrace();
		}
	}
}