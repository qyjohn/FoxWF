/**
 *
 * A simple utility to record timestamp and system load.
 *
 */
 
import java.lang.management.*;

public class LoadMonitor
{


	public static void main(String[] args)
	{
		int	   period = 1000; // default 1 second
		long   unixTime;
		double loadAvg;
		OperatingSystemMXBean mbean = ManagementFactory.getOperatingSystemMXBean();
		
		try
		{
			period = 1000 * Integer.parseInt(args[0]);	// The first argument is the sample period, default to 1 second.
		} catch (Exception e)	{}
		
		while (true)
		{
			try
			{
				unixTime = System.currentTimeMillis() / 1000L;
				loadAvg = mbean.getSystemLoadAverage();
				System.out.println(unixTime + "\t" + loadAvg);
				Thread.sleep(period);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}