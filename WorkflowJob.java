import java.util.HashSet;

public class WorkflowJob
{
	public String jobId, jobName, jobCommand;	// job id and job name
	public HashSet<String>	inputFiles, pendingInputFiles, outputFiles;
	public boolean ready;

	/**
	 *
	 * Constructor
	 *
	 */

	public WorkflowJob(String id, String name)
	{
		jobId = id;
		jobName = name;
		jobCommand = name;
		inputFiles = new HashSet<String>();
		pendingInputFiles = new HashSet<String>();
		outputFiles = new HashSet<String>();
		
		// By default this job is ready to go, unless we find out that file dependencies are not met
		ready = true;
	}
	
	
	/**
	 *
	 * Add arguments to the job
	 *
	 */
	
	public void addArgument(String args)
	{
		jobCommand = jobCommand + " " + args;
	}
	
	
	/**
	 *
	 * Add an input file dependency
	 *
	 */
	 
	public void addInputFile(String filename)
	{
		inputFiles.add(filename);
	}
	
	/**
	 *
	 * If a file is not yet available, add it to the pending input file set.
	 *
	 */

	public void addPendingInputFile(String filename)
	{
		pendingInputFiles.add(filename);
		ready = false;
	}
	
	/**
	 *
	 * During the execution of the workflow, we remove a file from the pending input file set when the file becomes available.
	 * This is usually trigger by the completion of a job, which produces the file.
	 *
	 * When this set becomes empty, it means that all the input files for this job are now available, and the job is ready to go.
	 *
	 */
	 
	public void removePendingInputFile(String filename)
	{
		pendingInputFiles.remove(filename);	
		if (pendingInputFiles.isEmpty())
		{
			ready = true;
		}
	}
	
	/**
	 *
	 * Add an output file. When the job has finished execution, we will update all the status of these files. 
	 *
	 */
	 
	public void addOutputFile(String filename)
	{
		outputFiles.add(filename);
	}
	
	/**
	 * 
	 * toString method
	 *
	 */
	 
	public String toString()
	{
		String str = jobId + "\t" + jobName;	// Job ID and name
		if (ready)
		{
			str = str + "\t (ready)";
		}
		else
		{
			str = str + "\t (not ready)";
		}
		
		str = str + "\n    " + jobCommand;
		 
		for (String file : inputFiles) 
		{
			str = str + "\n    " + file;
			if (pendingInputFiles.contains(file))
			{
				str = str + "\t (pending)";
			}
		}
		return str;
	}
	
	
}