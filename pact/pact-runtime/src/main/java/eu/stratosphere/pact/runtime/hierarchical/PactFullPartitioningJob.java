package eu.stratosphere.pact.runtime.hierarchical;

import java.io.IOException;



import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactInteger;
//import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
//import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class PactFullPartitioningJob 
{
	
	public static void runJob (JobParameters parameters)
	{
		JobGraph jobGraph = new JobGraph ("Full partitioning");
		
		final int degreeOfParallelism = 
				parameters.degreeOfParallelism;
		
		final int tasksPerInstance = 
				parameters.degreeOfParallelism / parameters.numberOfNodesInCluster;
		
		ChannelType channelType = null;
		if (parameters.localMode)
			channelType = ChannelType.INMEMORY;
		else {
			channelType = ChannelType.NETWORK;
			//channelType = ChannelType.FILE;
		}
		
		/*
		JobInputVertex input = JobGraphUtils.createFileInput (
				HierarchicalPartitioningInputFormat.class, 
				parameters.generatedDataPath + "/data", 
				"Input", jobGraph, degreeOfParallelism, tasksPerInstance);
		
		TaskConfig inputConfig = new TaskConfig (input.getConfiguration());
		inputConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
	    inputConfig.setComparatorFactoryForOutput (PactRecordComparatorFactory.class, 0);
	    PactRecordComparatorFactory.writeComparatorSetupToConfig (
	    		inputConfig.getConfigForOutputParameters(0),
	    		new int[] { 0 }, new Class[] { PactInteger.class }, new boolean[] { true });
		 */
		/*
		JobInputVertex input = JobGraphUtils.createInput(
				PactDataGenerationTask.class, 
				"Data generator", jobGraph, 
				degreeOfParallelism, tasksPerInstance);
				*/
		JobInputVertex input = JobGraphUtils.createInput(
				GenerationAndPartitioningTask.class, 
				"t0", jobGraph, 
				degreeOfParallelism, tasksPerInstance);
		
		input.getConfiguration().setBoolean("isHierarchicalPartitioning", false);
		input.getConfiguration().setInteger (
				"generatedRecordsPerTask", 
				parameters.recordsGeneratedPerTask);
		input.getConfiguration().setInteger (
				"maxKeyValue", parameters.maximumKeyValue);
		
		/*
		JobTaskVertex partitioner = JobGraphUtils.createTask (
				PactFullPartitioningTask.class, 
				"Partitioner", jobGraph, degreeOfParallelism, tasksPerInstance);
		*/
		
		
		JobOutputVertex output;
		if (parameters.writeOutput) {
			output = JobGraphUtils.createFileOutput (
					jobGraph, "t1", degreeOfParallelism, tasksPerInstance);
			TaskConfig outputConfig = new TaskConfig (output.getConfiguration());
			outputConfig.setStubClass (HierarchicalPartitioningOutputFormat.class);
		    outputConfig.setStubParameter (
		    		FileOutputFormat.FILE_PARAMETER_KEY, 
		    		parameters.outputPath + "/result");
		}
		else {
			output = JobGraphUtils.createDummyOutput (
					jobGraph, "t1", degreeOfParallelism, tasksPerInstance);
		}
		
		try {
			JobGraphUtils.connect (
					input, output, channelType, 
					DistributionPattern.BIPARTITE);
			/*
			JobGraphUtils.connect (
					input, partitioner, channelType, 
					DistributionPattern.POINTWISE);
			JobGraphUtils.connect(partitioner, output, channelType, 
					DistributionPattern.BIPARTITE);
					*/
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		output.setVertexToShareInstancesWith(input);
		//partitioner.setVertexToShareInstancesWith(input);
		
		
		
		GlobalConfiguration.loadConfiguration(parameters.configurationPath);
		Configuration conf = GlobalConfiguration.getConfiguration();
		
		try {
			JobGraphUtils.submit(jobGraph, conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit (-1);
		} catch (JobExecutionException e) {
			e.printStackTrace();
			System.exit (-1);
		}
	}

	public static void main (String [] args)
	{
		JobParameters parameters = new JobParameters ();
		
		parameters.setForLargeClusterMode();
		parameters.readFromCommandLine(args);
		
		runJob (parameters);
	}
}
