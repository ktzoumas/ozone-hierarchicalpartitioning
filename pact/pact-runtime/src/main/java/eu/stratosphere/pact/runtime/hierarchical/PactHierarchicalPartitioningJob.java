package eu.stratosphere.pact.runtime.hierarchical;

import java.io.IOException;


import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class PactHierarchicalPartitioningJob 
{
	
	public static void runJob (JobParameters parameters)
	{
		int n = parameters.degreeOfParallelism;
		/*
		while (n > 1)
			n /= parameters.channelsPerTask;
		if (n != 1) {
			System.err.println("Degree of parallelism (=" + 
					parameters.degreeOfParallelism 
					+ ") must be a power of channels per task (=" + 
					parameters.channelsPerTask + ")");
			System.exit(-1);
		}
		*/
		
		Class<? extends AbstractTask> partitioningTaskClass = null;
		if (parameters.useCombiningTasks)
			partitioningTaskClass = PactHierarchicalCombiningTask.class;
		else
			partitioningTaskClass = PactHierarchicalPartitioningTask.class;
		
		JobGraph jobGraph = new JobGraph ("Hierarchical partitioning");
		
		final int degreeOfParallelism = 
				parameters.degreeOfParallelism;
		
		final int tasksPerInstance = 
				parameters.degreeOfParallelism / parameters.numberOfNodesInCluster;
		
		ChannelType channelType = null;
		if (parameters.localMode)
			channelType = ChannelType.INMEMORY;
		else
			channelType = ChannelType.NETWORK;
		
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
		final JobInputVertex input = new JobInputVertex ("Input", jobGraph);
		input.setInputClass(PactDataGenerationTask.class);
		input.setNumberOfSubtasks(degreeOfParallelism);
		input.setNumberOfSubtasksPerInstance(tasksPerInstance);
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
		
		int taskNumberOfTeams = 1;
		//int taskNumberOfTeams = parameters.channelsPerTask;
		
		input.getConfiguration().setBoolean("isHierarchicalPartitioning", true);
		input.getConfiguration().setInteger (
				"generatedRecordsPerTask", 
				parameters.recordsGeneratedPerTask);
		input.getConfiguration().setInteger (
				"maxKeyValue", parameters.maximumKeyValue);
		input.getConfiguration().setInteger("degreeOfParallelism", degreeOfParallelism);
		input.getConfiguration().setInteger("numberOfTeams", taskNumberOfTeams);
		
		
		AbstractJobVertex previous = input;
		
		//int stages = //1 + 
		//		(int) (Math.log((double) parameters.degreeOfParallelism) / 
		//				Math.log((double) parameters.channelsPerTask)) - 1;
		int stages = parameters.numberOfStages;
		for (int i = 0; i < stages; i++) {
			String name = "t" + (i+1);
			
			//taskNumberOfTeams *= parameters.channelsPerTask[i];
			taskNumberOfTeams = parameters.channelsPerTask[i];
			
			JobTaskVertex task = JobGraphUtils.createTask (
					partitioningTaskClass, 
					name, jobGraph, degreeOfParallelism, tasksPerInstance);
			task.setNumberOfHierarchicalPartitioningTeams(taskNumberOfTeams);
			task.getConfiguration().setInteger("degreeOfParallelism", degreeOfParallelism);
			task.getConfiguration().setInteger("numberOfTeams", taskNumberOfTeams);
			
			try {
//				if (i == 0)
//					JobGraphUtils.connect(previous, task, channelType, 
//						DistributionPattern.POINTWISE);
//				else
					JobGraphUtils.connect(previous, task, channelType, 
							DistributionPattern.HIERARCHICAL_STEP);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
				System.exit(-1);
			}
			task.setVertexToShareInstancesWith(previous);
			
			previous = task;
			//taskNumberOfTeams *= parameters.channelsPerTask;
		}
		
		if (!parameters.writeOutput) {
			JobOutputVertex output;
			taskNumberOfTeams = degreeOfParallelism;
			output = JobGraphUtils.createDummyOutput (
					jobGraph, "t" + (stages+1), degreeOfParallelism, tasksPerInstance);
			output.setNumberOfHierarchicalPartitioningTeams(taskNumberOfTeams);
			output.getConfiguration().setInteger("degreeOfParallelism", degreeOfParallelism);
			output.getConfiguration().setInteger("numberOfTeams", taskNumberOfTeams);
			
			
			/*
			else {
				//taskNumberOfTeams *= parameters.channelsPerTask[stages-1];
				taskNumberOfTeams = degreeOfParallelism;
				output = JobGraphUtils.createDummyOutput (
						jobGraph, "t" + (stages+1), degreeOfParallelism, tasksPerInstance);
				output.setNumberOfHierarchicalPartitioningTeams(taskNumberOfTeams);
				output.getConfiguration().setInteger("degreeOfParallelism", degreeOfParallelism);
				output.getConfiguration().setInteger("numberOfTeams", taskNumberOfTeams);
			}
			*/
			/*
			TaskConfig outputConfig = new TaskConfig (output.getConfiguration());
			outputConfig.setStubClass (HierarchicalPartitioningOutputFormat.class);
		    outputConfig.setStubParameter (
		    		FileOutputFormat.FILE_PARAMETER_KEY, 
		    		parameters.outputPath + "/result");
		    */
			
		    try {
				JobGraphUtils.connect (
						previous, output, channelType, 
						//DistributionPattern.POINTWISE);
						DistributionPattern.HIERARCHICAL_STEP);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
				System.exit(-1);
			}
			
			output.setVertexToShareInstancesWith(previous);
		}
		else {
			taskNumberOfTeams = degreeOfParallelism;
			JobTaskVertex finalTask = JobGraphUtils.createTask (
					partitioningTaskClass, 
					"t" + (stages+1), jobGraph, degreeOfParallelism, tasksPerInstance);
			finalTask.setNumberOfHierarchicalPartitioningTeams(taskNumberOfTeams);
			finalTask.getConfiguration().setInteger("degreeOfParallelism", degreeOfParallelism);
			finalTask.getConfiguration().setInteger("numberOfTeams", taskNumberOfTeams);
			
			
			JobOutputVertex writer = JobGraphUtils.createFileOutput (
					jobGraph, "t" + (stages+2), degreeOfParallelism, tasksPerInstance);
			TaskConfig outputConfig = new TaskConfig (writer.getConfiguration());
			outputConfig.setStubClass (HierarchicalPartitioningOutputFormat.class);
		    outputConfig.setStubParameter (
		    		FileOutputFormat.FILE_PARAMETER_KEY, 
		    		parameters.outputPath + "/result");
		    try {

		    	JobGraphUtils.connect (
						previous, finalTask, channelType, 
						//DistributionPattern.POINTWISE);
						DistributionPattern.HIERARCHICAL_STEP);
		    	JobGraphUtils.connect (
						finalTask, writer, channelType, 
						//DistributionPattern.POINTWISE);
						DistributionPattern.POINTWISE);
		    }
		    catch (JobGraphDefinitionException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		    finalTask.setVertexToShareInstancesWith(previous);
		    writer.setVertexToShareInstancesWith(finalTask);
		}
		
		
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
		//parameters.setForLocalMode();
		parameters.readFromCommandLine(args);
		
		System.out.println (parameters.toString());
		
		runJob (parameters);
	}
}
