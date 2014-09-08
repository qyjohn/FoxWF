import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;


public class Workflow
{

	HashMap<String, WorkflowJob> initialJobs, queueJobs, runningJobs, completeJobs;
	String projectDir;
	
	/**
	 *
	 * Constructor
	 *
	 */
	 
	public Workflow(String dir)
	{
		try
		{
			// Initialize the HashMap for workflow jobs
			initialJobs = new HashMap<String, WorkflowJob>();
			queueJobs = new HashMap<String, WorkflowJob>();
			runningJobs = new HashMap<String, WorkflowJob>();
			completeJobs = new HashMap<String, WorkflowJob>();

			projectDir = dir;
			String fullPath = "file:///" + projectDir + "/dag.xml";
			Document doc = parseDocument(fullPath);
			parseWorkflow(doc);			
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
	}
	
	/**
	 *
	 * Parse the work flow from dag.xml.
	 *
	 */
         
	public Document parseDocument(String fullPath) throws Exception 
	{
		SAXReader reader = new SAXReader();
		URL url = new URL(fullPath);
		Document document = reader.read(url);
		return document;
	}
	
	
	/**
	 *
	 * Parse jobs and job dependencies
	 *
	 */
	 
	public void parseWorkflow(Document doc)
	{
		List<Element> jobs = doc.getRootElement().elements("job");
		List<Element> children = doc.getRootElement().elements("child");

		for(Element job : jobs) 
		{
			prepareJob(job);
		}
		for(Element child : children) 
		{
			prepareChild(child);
		}
	}
	
	
	/**
	 *
	 * Parse the dependencies of a job
	 *
	 */
	 
	public void prepareChild(Element child)
	{
		String child_id = child.attribute("ref").getValue();
		List<Element> parents = child.elements("parent");
		
		for (Element parent: parents)
		{
			String parent_id = parent.attribute("ref").getValue();
			initialJobs.get(child_id).addParent(parent_id);
			initialJobs.get(parent_id).addChild(child_id);
		}
	}

	
	/**
	 *
	 * Parse a job, extract job name (command) and command line arguments
	 *
	 */
	 
	public void prepareJob(Element job)
	{
		String id = job.attribute("id").getValue();
		String name = job.attribute("name").getValue();
		
		WorkflowJob wlj = new WorkflowJob(id, name);
		Element args = job.element("argument");
		for ( int i = 0, size = args.nodeCount(); i < size; i++ )
		{
			Node node = args.node(i);
			if ( node instanceof Element ) 
			{
                Element e = (Element) node;
                wlj.addArgument(e.attribute("file").getValue());
            }
            else
            {
	            StringTokenizer st = new StringTokenizer(node.getText().trim());
				while (st.hasMoreTokens()) 
				{
					wlj.addArgument(st.nextToken());
				}
            }
		}
	
		initialJobs.put(id, wlj);
	}
}