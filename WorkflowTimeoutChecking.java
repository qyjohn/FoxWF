import java.util.HashMap;

public class WorkflowTimeoutChecking extends Thread
{
	HashMap<String, WorkflowScheduler> allWorkflows;
	int timeout;
	
	public WorkflowTimeoutChecking(HashMap<String, WorkflowScheduler> wf, int t)
	{
		allWorkflows = wf;
		timeout = t;
	}
	
	
	public void run()
	{
		while (true)
		{
			try
			{	
				long current = System.currentTimeMillis() / 1000L;
				// Traverse through all workflows
				for (String key1 : allWorkflows.keySet()) 
				{
					// Get the running jobs in the workflow
					HashMap<String, WorkflowJob> runningJobs = allWorkflows.get(key1).wf.runningJobs;
					// Traverse through all running jobs
					for (String key2 : runningJobs.keySet())
					{
						WorkflowJob job = runningJobs.get(key2);
						// Check how long this has been running
						int job_timeout = job.timeout;
						int job_runtime = (int) (current - job.start_time);
						// Check if there is a timeout
						if (job_runtime > job_timeout)
						{
							System.out.println(job.jobId + "\t" + job_timeout + "\t" + job_runtime + "\t timeout....");
						}
					}
				}				
				sleep(10000);	// sleep 10 seconds
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}