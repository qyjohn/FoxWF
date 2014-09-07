import java.util.HashMap;
import org.dom4j.*;
import java.util.UUID;

public class PrjMQ extends Thread
{
	PullMQ pmq;		// Project MQ
	PushMQ jmq;		// Job MQ
	FoxDB database;
	HashMap<String, WorkflowScheduler> allWorkflows;
	
	/**
	 *
	 * Constructor
	 *
	 * This constructor is used to create an AckMQ listening for ACK message.
	 *
	 * @param	port	the port number to listen for ACK messages
	 * @param	wf		the HashMap containing all the workflows
	 *
	 */
	 
	public PrjMQ(HashMap<String, WorkflowScheduler> wf, FoxDB db, PushMQ mq)
	{
		allWorkflows = wf;
		database = db;
		jmq = mq;
		pmq = new PullMQ("localhost", JobMQ.SIMPLE_WORKFLOW_PRJ_MQ);
	}

	
	
	/**
	 *
	 * run() method of the thread.
	 * It receives ACK message from the AckMQ, and set the appropriate job as either running or complete. 
	 *
	 */
	 
	public synchronized void run()
	{
		while (true)
		{
			try
			{
				// Receive project message 
				String prjString = pmq.pullMQ();
				System.out.println(prjString);
				Element prj = DocumentHelper.parseText(prjString).getRootElement();
				String name = prj.attribute("name").getValue();
				String path   = prj.attribute("path").getValue();

				// Create a new workflow scheduler to 	
		    	String uuid = UUID.randomUUID().toString();
		    	database.add_workflow(uuid, name, path);
				WorkflowScheduler scheduler = new WorkflowScheduler(database, uuid, jmq, path);
				allWorkflows.put(uuid, scheduler);
				database.update_workflow(uuid, "started");
				scheduler.initialDispatch();
				
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();	
			}
		}		
	}
}