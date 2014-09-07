import java.util.HashSet;


public class WorkflowFile
{
	public String filename;	// File name
	HashSet<String>	jobs;	// Job that depends on this file

	/**
	 *
	 * Constructor
	 *
	 */
	 
	public WorkflowFile(String name)
	{
		filename = name;
		jobs = new HashSet<String>();
	}
	
	/**
	 *
	 * Add a job that depends on the file
	 *
	 */
	 
	public void addJob(String id)
	{
		jobs.add(id);
	}
	
	/**
	 *
	 * Remove a job from the dependency list when the job has been push to the queue for execution.
	 *
	 */
	 
	public void delJob(String id)
	{
		jobs.remove(id);
	}
	
	/**
	 * 
	 * toString method
	 *
	 */
	 
	public String toString()
	{
		String str = filename;
		for (String job : jobs)
		{
			str = str + "\n    " + job;
		}
/*		for (String key : jobs.keySet()) 
		{
			str = str + "\n    " + key;
		}
*/		
		return str;
	}

}