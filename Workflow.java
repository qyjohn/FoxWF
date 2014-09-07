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
	HashMap<String, WorkflowFile> inputFiles, inoutFiles, outputFiles;
	String projectDir;
	
	public Workflow(String dir)
	{
		try
		{
			// Initialize the HashMap for workflow files
			inputFiles = new HashMap<String, WorkflowFile>();
			inoutFiles = new HashMap<String, WorkflowFile>();
			outputFiles = new HashMap<String, WorkflowFile>();
			
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
	
	
	public void parseWorkflow(Document doc)
	{
		List<Element> files = doc.getRootElement().elements("filename");
		List<Element> jobs = doc.getRootElement().elements("job");
		for(Element file : files) 
		{
			prepareFile(file);
		}
		for(Element job : jobs) 
		{
			prepareJob(job);
		}
	}
	
	/**
	 *
	 *
	 */
	 
	public void prepareFile(Element file)
	{
		String filename = file.attribute("file").getValue();
		String link = file.attribute("link").getValue();	// file type, can be 'input', 'output' or 'inout'
		
		WorkflowFile wlf = new WorkflowFile(filename);
		if (link.equals("input"))
		{
			inputFiles.put(filename, wlf);
		}
		else if (link.equals("inout"))
		{
			inoutFiles.put(filename, wlf);
		}
		else if (link.equals("output"))
		{
			outputFiles.put(filename, wlf);
		}
	}
	
	
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
		
		List<Element> uses = job.elements("uses");
		for(Element use : uses) 
		{
			String filename = use.attribute("file").getValue();
			String link = use.attribute("link").getValue();	// file type, can be 'input', 'output' or 'inout'
			
			if (link.equals("input"))
			{
				// Filter out executable files, these should be prepared by user rather than job scheduler
				String type = "data";
				if (use.attribute("type") != null)
				{
					type = use.attribute("type").getValue();
				}
				
				if (type.equals("data"))
				{
					wlj.addInputFile(filename);
					if (inoutFiles.containsKey(filename))
					{
						wlj.addPendingInputFile(filename);
						inoutFiles.get(filename).addJob(id);
					}
				}
			}
			else if (link.equals("output"))
			{
				wlj.addOutputFile(filename);
			}
		}
		
		initialJobs.put(id, wlj);
	}
	
	public void listFiles()
	{
		for (WorkflowFile file : inoutFiles.values()) 
		{
			System.out.println(file);
		}
	}

	public void listJobs(String type)
	{
		if (type.equals("initial"))
		{
			for (WorkflowJob job : initialJobs.values()) 
			{
				System.out.println(job);
			}
		}
		else if (type.equals("queue"))
		{
			for (WorkflowJob job : queueJobs.values()) 
			{
				System.out.println(job);
			}
		}
		else if (type.equals("running"))
		{
			for (WorkflowJob job : runningJobs.values()) 
			{
				System.out.println(job);
			}
		}
		else if (type.equals("complete"))
		{
			for (WorkflowJob job : completeJobs.values()) 
			{
				System.out.println(job);
			}
		}
	}
	
	public static void main(String args[])
	{
		Workflow wf = new Workflow("/data/2.0_Montage");
		wf.listFiles();
		wf.listJobs("initial");
	}

	
}