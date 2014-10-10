import java.util.HashSet;
import java.util.LinkedList;

public class WorkflowScheduler
{
	FoxDB database;
	Workflow wf;
	PushMQ mq;
	int timeout;
	String uuid, projectPath;
	HashSet<String> initialSet, pendingSet, runningSet;
	String jobInfo;
	boolean completed;
	
	public WorkflowScheduler(FoxDB db, String id, PushMQ m, String path, int t)
	{
		database = db;
		uuid = id;
		projectPath = path;
		mq = m;
		timeout = t;
		
		completed  = false;
		initialSet = new HashSet<String>();
		pendingSet = new HashSet<String>();
		runningSet = new HashSet<String>();
		
		wf = new Workflow(path, timeout);
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
/*
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
*/

		for (WorkflowJob job : wf.initialJobs.values())	
		{
			if (job.ready)
			{
				pendingSet.add(job.jobId);
				jobInfo = createJobInfo(job.jobId, job.jobCommand);
				mq.pushMQ(jobInfo);
			}
			else
			{
				initialSet.add(job.jobId);
			}
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
/*		// Get the job from the initialJobs HashSet
		if (wf.initialJobs.containsKey(id))
		{
			WorkflowJob job = wf.initialJobs.get(id);
			wf.queueJobs.put(job.jobId, job);	
			wf.initialJobs.remove(id);		
	
			// Publish the job to the queueJobs MQ, and move the job to the queueJobs HashSet
			String jobInfo = createJobInfo(job.jobId, job.jobCommand);
			mq.pushMQ(jobInfo);			
		}
*/
		if (initialSet.contains(id))
		{
			WorkflowJob job = wf.initialJobs.get(id);
			jobInfo = createJobInfo(job.jobId, job.jobCommand);
			mq.pushMQ(jobInfo);	
			initialSet.remove(id);
			pendingSet.add(id);
		}		
	}
	
	
	/**
	 *
	 * The worker node sends an ACK message to the AckMQ, indicating a particular job is now running. 
	 * Move the job from queueJobs HashMap to runningJobs HashMap
	 *
	 */
	 
	public synchronized void setJobAsRunning(String id, String worker)
	{
/*		if (wf.queueJobs.containsKey(id))
		{
			System.out.println(uuid + ":\t" + id + " is running on worker " + worker + ".");
			WorkflowJob job = wf.queueJobs.get(id);
			job.start_time = System.currentTimeMillis() / 1000L;	// This is when this particular job is started, need this for job execution timeout
			wf.runningJobs.put(id, job);
			wf.queueJobs.remove(id);
			
			// Update the job status in the database
			database.update_job_running(uuid, id, worker);
		}
*/
		if (pendingSet.contains(id))
		{
			System.out.println(uuid + ":\t" + id + " is running on worker " + worker + ".");

			pendingSet.remove(id);
			runningSet.add(id);
			database.update_job_running(uuid, id, worker);
		}		

		
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
/*
		if (wf.runningJobs.containsKey(id))
		{
			System.out.println(uuid + ":\t" + id + " is complete.");
			// Get the current job with job id
			WorkflowJob job = wf.runningJobs.get(id);
			
			// Get a list of the children jobs
			for (String child_id : job.childrenJobs) 
			{
				// Get a list of the jobs depending on a particular output file
				WorkflowJob childJob = wf.initialJobs.get(child_id);
				// Remove this depending parent job
				childJob.removeParent(id);
				if (childJob.ready)
				{
					// No more pending input files, this job is now ready to go
					System.out.println(uuid + ":\t" + childJob.jobId + " is now ready to go. Dispatching...");
					dispatchJob(childJob.jobId);
				}
			}
			
			// Update the job status in the database
			database.update_job_completed(uuid, id);
			
			// Move the job from runningJobs HashMap to completeJobs HashMap
			// 2014-10-10, do not add a completed job to the completeJobs HashMap, to save some memory
//			wf.completeJobs.put(id, job);
			wf.runningJobs.remove(id);
			
			// Check if the workflow is completed
			if ((wf.initialJobs.size() == 0) && (wf.queueJobs.size() == 0) && (wf.runningJobs.size() == 0))
			{				
				System.out.println(uuid + ":\t" +  "[COMPLETED]");
				database.update_workflow(uuid, "completed");
			}
		}
*/
		if (runningSet.contains(id))
		{
			runningSet.remove(id);
			database.update_job_completed(uuid, id);
			
			WorkflowJob job = wf.initialJobs.get(id);
			
			// Get a list of the children jobs
			for (String child_id : job.childrenJobs) 
			{
				// Get a list of the jobs depending on a particular output file
				WorkflowJob childJob = wf.initialJobs.get(child_id);
				// Remove this depending parent job
				childJob.removeParent(id);
				if (childJob.ready)
				{
					// No more pending input files, this job is now ready to go
					System.out.println(uuid + ":\t" + childJob.jobId + " is now ready to go. Dispatching...");
					dispatchJob(childJob.jobId);
				}
			}
			
			// Delete this job from initialJobs
			wf.initialJobs.remove(id);
			
			
			// Check if the workflow is completed
			if ((initialSet.size() == 0) && (pendingSet.size() == 0) && (runningSet.size() == 0))
			{				
				completed = true;
				initialSet = null;
				pendingSet = null;
				runningSet = null;
				wf = null;
				
				System.out.println(uuid + ":\t" +  "[COMPLETED]");
				database.update_workflow(uuid, "completed");
			}						
		}		

		
	}
	
	
	/**
	 *
	 * After a worker takes a particular job for a certain time, but does not ACK this job as complete, the
	 * job is considered as "timeout". Need to dispatch the job again so that another worker node can process
	 * it a second time.
	 *
	 * Remove the job from the runningJobs HashMap, need to pushMQ, and put it back to the queueJobs HashMap.
	 *
	 */
	 
	public synchronized void handleJobTimeout(String id)
	{
/*
		// Move the job from runningJobs HashMap to queueJobs HashMap
		if (wf.runningJobs.containsKey(id))
		{
			WorkflowJob job = wf.runningJobs.get(id);
			wf.queueJobs.put(id, job);
			wf.runningJobs.remove(id);
	
			// Publish a second message to the Job MQ
			String jobInfo = createJobInfo(job.jobId, job.jobCommand);
			mq.pushMQ(jobInfo);						

			System.out.println(uuid + ":\t" + id + " is now re-submit for execution.");
		}
*/

		if (runningSet.contains(id))
		{
			runningSet.remove(id);
			pendingSet.add(id);
			
			WorkflowJob job = wf.initialJobs.get(id);
			jobInfo = createJobInfo(job.jobId, job.jobCommand);
			mq.pushMQ(jobInfo);						
			System.out.println(uuid + ":\t" + id + " is now re-submit for execution.");
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