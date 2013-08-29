package eu.stratosphere.pact.runtime.hierarchical;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.io.FileInputFormat;
//import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class JobGraphUtils 
{
	
	//public static String profilingDataFileName = "hdfs://cloud-11.dima.tu-berlin.de:40010/kostas/profiling/nephele-profiling-data.out";
	
	public static String profilingDataFileName = "/home/hadoop/kostas-profiling-data/nephele-profiling-data.out";

	public static void submit (JobGraph graph, Configuration nepheleConfig) 
				throws IOException, JobExecutionException 
	{
	    JobClient client = new JobClient(graph, nepheleConfig);
	    client.getConfiguration().setString("profilingDataFileName", profilingDataFileName);
	    client.submitJobAndWait();
	}
	
	
	public static JobInputVertex createInput (
			Class<? extends AbstractInputTask<?>> task, String name, JobGraph graph,
		    int degreeOfParallelism, int numSubtasksPerInstance)
	{
		JobInputVertex taskVertex = new JobInputVertex(name, graph);
		taskVertex.setInputClass(task);
		taskVertex.setNumberOfSubtasks(degreeOfParallelism);
		taskVertex.setNumberOfSubtasksPerInstance(numSubtasksPerInstance);
		return taskVertex;
	}
	
	public static JobInputVertex createFileInput (
			Class<?> stubClass, String path, String name, JobGraph graph,
		    int degreeOfParallelism, int numSubTasksPerInstance) 
	{
	    JobInputVertex inputVertex = new JobInputVertex(name, graph);
	    Class<AbstractInputTask<?>> clazz = 
	    		(Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
	    inputVertex.setInputClass(clazz);
	    inputVertex.setNumberOfSubtasks(degreeOfParallelism);
	    inputVertex.setNumberOfSubtasksPerInstance(numSubTasksPerInstance);
	    TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
	    inputConfig.setStubClass(stubClass);
	    inputConfig.setStubParameter(FileInputFormat.FILE_PARAMETER_KEY, path);
	    return inputVertex;
	}
	
	public static void connect (
			AbstractJobVertex source, AbstractJobVertex target, ChannelType channelType,
		    DistributionPattern distributionPattern) 
		    		throws JobGraphDefinitionException 
	{
	    source.connectTo (
	    			target, channelType, 
	    			CompressionLevel.NO_COMPRESSION, distributionPattern);
	}
	
	
	public static JobOutputVertex createDummyOutput (
			JobGraph jobGraph, String name, int degreeOfParallelism,
			int numSubTasksPerInstance) 
	{
	    JobOutputVertex sinkVertex = new JobOutputVertex(name, jobGraph);
	    sinkVertex.setOutputClass(DummySinkTask.class);
	    sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
	    sinkVertex.setNumberOfSubtasksPerInstance(numSubTasksPerInstance);
	    return sinkVertex;
	}

	public static JobOutputVertex createFileOutput (
			JobGraph jobGraph, String name, int degreeOfParallelism,
			int numSubTasksPerInstance) 
	{
	    JobOutputVertex sinkVertex = new JobOutputVertex(name, jobGraph);
	    sinkVertex.setOutputClass(DataSinkTask.class);
	    sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
	    sinkVertex.setNumberOfSubtasksPerInstance(numSubTasksPerInstance);
	    return sinkVertex;
	}
	
	public static JobTaskVertex createTask (
			Class<? extends AbstractTask> task, String name, JobGraph graph,
		    int degreeOfParallelism, int numSubtasksPerInstance) 
	{
		JobTaskVertex taskVertex = new JobTaskVertex(name, graph);
		taskVertex.setTaskClass(task);
		taskVertex.setNumberOfSubtasks(degreeOfParallelism);
		taskVertex.setNumberOfSubtasksPerInstance(numSubtasksPerInstance);
		return taskVertex;
	}
	
}
