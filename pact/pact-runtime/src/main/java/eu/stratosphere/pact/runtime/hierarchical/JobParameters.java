package eu.stratosphere.pact.runtime.hierarchical;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.util.StringUtils;

public final class JobParameters {


	public boolean localMode;

	/**
	 * The number of times the job execution shall be repeated.
	 */
	public int numberOfRuns = 1;

	/**
	 * The number of consumers to create for the job execution.
	 */
	public int degreeOfParallelism = 64;

	/**
	 * How many input channels per task
	 */
	public int [] channelsPerTask = null;
	
	public int numberOfStages = -1	;
	
	/**
	 * The number of records to send in one run.
	 */
	public int recordsGeneratedPerTask = 1000;

	/**
	 * The path to write the output to.
	 */
	public String outputPath = null;
	
	public String generatedDataPath = null;

	//public static int MAX_KEY_VALUE = DEGREE_OF_PARALLELISM * 1000;
	
	public int maximumKeyValue = degreeOfParallelism;

	public String configurationPath = null;
	
	public int numberOfNodesInCluster = -1;
	
	//public static int NUMBER_OF_NETWORK_BUFFERS;
	
	//public static int SIZE_OF_NETWORK_BUFFER;
	
	//public static int 
	
	public boolean writeOutput;
	
	public boolean useCombiningTasks;
	
	public JobParameters ()
	{
		
	}
	
	public void setForLocalMode ()
	{
		localMode = true;
		numberOfNodesInCluster = 1;
		generatedDataPath = "file:///Users/kostas/temp/";
		outputPath = "file:///Users/kostas/temp/";
		numberOfNodesInCluster = 1;
		configurationPath = "/Users/kostas/Dropbox/stratosphere-dev/v02-mac/stratosphere/stratosphere-dist/src/main/stratosphere-bin/conf";
	}
	
	public void setForSmallClusterMode ()
	{
		localMode = false;
		numberOfNodesInCluster = 4;
		generatedDataPath = "hdfs://cloud-7.dima.tu-berlin.de:40010/kostas";
		outputPath = "hdfs://cloud-7.dima.tu-berlin.de:40010/kostas";
		//GENERATED_DATA_PATH = "file:///home/kostas";
		//OUTPUT_PATH = "file:///home/kostas";
		//CONFIGURATION_PATH = "/Users/kostas/Dropbox/stratosphere-dev";
		configurationPath = "/share/nephele/ozone-kostas/conf";
	}
	
	public void setForLargeClusterMode ()
	{
		localMode = false;
		numberOfNodesInCluster = 26;
		generatedDataPath = "hdfs://cloud-11.dima.tu-berlin.de:40010/kostas";
		outputPath = "hdfs://cloud-11.dima.tu-berlin.de:40010/kostas";
		//GENERATED_DATA_PATH = "file:///home/kostas";
		//OUTPUT_PATH = "file:///home/kostas";
		//CONFIGURATION_PATH = "/Users/kostas/Dropbox/stratosphere-dev";
		configurationPath = "/share/hadoop/stratosphere/ozone-kostas/conf";
	}
	
	public void setForWallyClusterMode ()
	{
		localMode = false;
		numberOfNodesInCluster = 200;
		generatedDataPath = null;
		outputPath = null;
		configurationPath = "/home/kostas.tzoumas/ozone-kostas/conf";
	}
	
	public void readFromCommandLine (String [] args)
	{
		if (args.length < 7) {
			System.err.println("Please specify at least the following parameters:");
			System.err.println(
					"<number of runs> <degree of parallelism> <stages> [<channels per taks for each stage> (as many numbers as stages)] <records generated per task> <maximum key value>" +
					"<write output to disk> <aggregate in intermediate stages>\n" +
					"Example: 1 10 2 5 2 100 false false");
			System.exit(-1);
		}
		
		try {
			numberOfRuns = Integer.parseInt(args[0]);
			degreeOfParallelism = Integer.parseInt(args[1]);
			numberOfStages = Integer.parseInt(args[2]) - 1;
			channelsPerTask = new int [numberOfStages];
			int prev = 1;
			for (int i = 0; i < numberOfStages; i++) {
				channelsPerTask[i] = Integer.parseInt(args[3+i]) * prev;
				prev = channelsPerTask[i];
			}
			int finalNumTasks = channelsPerTask[numberOfStages-1] * Integer.parseInt(args[3+numberOfStages]);
			//int finalNumTasks = channelsPerTask[numberOfStages-1];
			if (finalNumTasks != degreeOfParallelism) {
				System.err.println("Product of channels does not equal degree of parallelism " + numberOfRuns + ", " + 
						numberOfStages + ", " + channelsPerTask.toString() + ", " + Integer.parseInt(args[3+numberOfStages]) + ", " + finalNumTasks + ", " + degreeOfParallelism);
				System.exit(-1);
			}
			int n = 4 + numberOfStages;
			recordsGeneratedPerTask = Integer.parseInt(args[n]);
			//OUTPUT_PATH = args[4];
			maximumKeyValue = Integer.parseInt(args[n+1]);
			writeOutput = Boolean.parseBoolean(args[n+2]);
			useCombiningTasks = Boolean.parseBoolean(args[n+3]);
			//CONFIGURATION_PATH = args[6];
			//NUMBER_OF_NODES = Integer.parseInt(args[7]);
		} catch (NumberFormatException nfe) {
			nfe.printStackTrace();
			System.exit(-1);
		}
	}
	
	@Override
	public String toString() {
		return "JobParameters [localMode=" + localMode + ", numberOfRuns="
				+ numberOfRuns + ", degreeOfParallelism=" + degreeOfParallelism
				+ ", channelsPerTask=" + Arrays.toString(channelsPerTask)
				+ ", numberOfStages=" + numberOfStages
				+ ", recordsGeneratedPerTask=" + recordsGeneratedPerTask
				+ ", outputPath=" + outputPath + ", generatedDataPath="
				+ generatedDataPath + ", maximumKeyValue=" + maximumKeyValue
				+ ", configurationPath=" + configurationPath
				+ ", numberOfNodesInCluster=" + numberOfNodesInCluster
				+ ", writeOutput=" + writeOutput + ", useCombiningTasks="
				+ useCombiningTasks + "]";
	}
	
	/*
	public void getRuntimeStatisticsFromJobManager (InetAddress jobManagerAddress)
	{
		final InetSocketAddress profilingAddress = 
				new InetSocketAddress(jobManagerAddress, GlobalConfiguration
				.getInteger(ProfilingUtils.JOBMANAGER_RPC_PORT_KEY, ProfilingUtils.JOBMANAGER_DEFAULT_RPC_PORT));
		ProfilerImplProtocol jobManagerProfilerTmp = null;
		try {
			jobManagerProfilerTmp = 
					(ProfilerImplProtocol) RPC.getProxy(
							ProfilerImplProtocol.class, 
							profilingAddress,
				NetUtils.getSocketFactory());
		} catch (IOException e) {
			throw new ProfilingException(StringUtils.stringifyException(e));
		}
		this.jobManagerProfiler = jobManagerProfilerTmp;
	}
	*/
}
