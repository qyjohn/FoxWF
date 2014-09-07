import java.util.HashSet;
import java.util.LinkedList;

public class WorkflowScheduler
{
	FoxDB database;
	Workflow wf;
	PushMQ mq;
	String uuid, projectPath;
	
	public WorkflowScheduler(FoxDB db, String id, PushMQ m, String path)
	{
		database = db;
		uuid = id;
		projectPath = path;
		mq = m;
		
		wf = new Workflow(path);
		for (WorkflowJob job : wf.initialJobs.values()) 
		{
			// Register the job to the database
			database.add_job(uuid, job.jobId, job.jobName);
		}		
	}
	
	
	public void initialDispatch()
	{
		HashSet<String> queueSet = new HashSet<String>();
		LinkedList<String>	pendingPush = new LinkedList<String>();

		for (WorkflowJob job : wf.initialJobs.values()) 
		{
			if (job.ready)
			{
				// Push job information to MQ
				String jobInfo = createJobInfo(job.jobId, job.jobCommand);
				pendingPush.add(jobInfo);
				
				// Move job from initial job HashMap to queue job HashMap
				queueSet.add(job.jobId);
				// This job will need to be deleted from the initialJobs HashMap, let's do it in a batch at the end of the initial dispatch.
				wf.queueJobs.put(job.jobId, job);					
			}
		}

		// Delete all queue jobs from the initialJobs HashMap
		for (String id : queueSet) 
		{
			wf.initialJobs.remove(id);
		}
		
		// Now push the job info to JobMQ
		for (String jobInfo : pendingPush)
		{
			mq.pushMQ(jobInfo);
		}
	}
	
	
	/**
	 *
	 * Dispatch a single job from the initialJobs HashSet to the queueJobs HashSet. Jobs in the queueJobs HashSet will be
	 * pulled by the worker nodes for execution.
	 *
	 */
	 
	public synchronized void dispatchJob(String id)
	{
		// Get the job from the initialJobs HashSet
		WorkflowJob job = wf.initialJobs.get(id);

		// Publish the job to the queueJobs MQ, and move the job to the queueJobs HashSet
		String jobInfo = createJobInfo(job.jobId, job.jobCommand);
		mq.pushMQ(jobInfo);
		wf.queueJobs.put(job.jobId, job);	
				
		// Remove the job from the initialJobs HashSet
		wf.initialJobs.remove(id);		
	}
	
	
	/**
	 *
	 * The worker node sends an ACK message to the AckMQ, indicating a particular job is now running. 
	 * Move the job from queueJobs HashMap to runningJobs HashMap
	 *
	 */
	 
	public synchronized void setJobAsRunning(String id, String worker)
	{
		System.out.println(uuid + ":\t" + id + " is running on worker " + worker + ".");
		WorkflowJob job = wf.queueJobs.get(id);
		wf.runningJobs.put(id, job);
		wf.queueJobs.remove(id);
		
		// Update the job status in the database
		database.update_job_running(uuid, id, worker);
	}
	
	
	/**
	 *
	 * The worker node sends an ACK message to the AckMQ, indicating a particular job is now complete.
	 * There are several things to process, including:
	 * (1) obtain a list of the output files of this particular job
	 * (2) for each output file, find the jobs that depend on this output file
	 * (3) for each job that depends on this output file, check if it is now ready to run, and dispatch it if it is ready
	 * (4) move the job from runningJobs HashMap to completeJobs HashMap.
	 *
	 */
	 
	public synchronized void setJobAsComplete(String id, String worker)
	{		
		System.out.println(uuid + ":\t" + id + " is complete.");
		// Get the current job with job id
		WorkflowJob job = wf.runningJobs.get(id);
		// Get a list of the output files
		for (String file : job.outputFiles) 
		{
			// Get a list of the jobs depending on a particular output file
			if (wf.inoutFiles.get(file) != null)
			{
				for (String dependingJobId : wf.inoutFiles.get(file).jobs)
				{
					WorkflowJob dependingJob = wf.initialJobs.get(dependingJobId);
					dependingJob.pendingInputFiles.remove(file);
					if (dependingJob.pendingInputFiles.isEmpty())
					{
						// No more pending input files, this job is now ready to go
						System.out.println(uuid + ":\t" + dependingJob.jobId + " is now ready to go. Dispatching...");
						dispatchJob(dependingJob.jobId);
					}
				}
			}
		}
		
		// Update the job status in the database
		database.update_job_completed(uuid, id);
		
		// Move the job from runningJobs HashMap to completeJobs HashMap
		wf.completeJobs.put(id, job);
		wf.runningJobs.remove(id);
		
		// Check if the workflow is completed
		if ((wf.initialJobs.size() == 0) && (wf.queueJobs.size() == 0) && (wf.runningJobs.size() == 0))
		{				
			System.out.println(uuid + ":\t" +  "[COMPLETED]");
			database.update_workflow(uuid, "completed");
		}
	}
	
	/**
	 *
	 * Create an MQ message to be pushed to the job queue
	 *
	 */
	 
	public String createJobInfo(String id, String command)
	{
		String info = "<job project='" + uuid + "' id='" + id + "' path='" + projectPath + "'>\n";
		info = info + "<command>\n";
		info = info + command + "\n";
		info = info + "</command>\n";
		info = info + "</job>";

		return info;		
	}
}