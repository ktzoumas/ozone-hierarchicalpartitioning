package eu.stratosphere.pact.runtime.hierarchical;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class PactHierarchicalHashChannelSelector implements ChannelSelector<PactRecord>{

	private int lowOutputTaskIndex;
	private int highOutputTaskIndex;
	private int numberOfOutputTasks;
	
	private int [] channels = {0};
	
	public PactHierarchicalHashChannelSelector (int lowOutputTaskIndex, int highOutputTaskIndex, int numberOfOutputTasks)
	{
		this.lowOutputTaskIndex = lowOutputTaskIndex;
		this.highOutputTaskIndex = highOutputTaskIndex;
		this.numberOfOutputTasks = numberOfOutputTasks;
	}
	
	@Override
	public int[] selectChannels(PactRecord record, int numberOfOutputChannels) {
		
		PactInteger hashInt = record.getField(0, PactInteger.class);
		
		
		int hash = hashInt.getValue();
		
		
		int outputTaskIndex = (hash < 0) 
				? -hash % numberOfOutputTasks 
						: hash % numberOfOutputTasks;
		
		// Divide index range to {@link numberOfChannels} equal ranges
		// that correspond to different teams, and send record accordingly
		
		int indicesPerChannel = 
				(highOutputTaskIndex - lowOutputTaskIndex + 1) / numberOfOutputChannels;
		
		channels[0] = (outputTaskIndex - lowOutputTaskIndex) / indicesPerChannel;
		
		if (channels[0] < 0)
			System.out.println("Hierarchical channel selector: " + hashInt.getValue() + " " + hash + " " + outputTaskIndex + " " + " " + 
		numberOfOutputChannels + " " + lowOutputTaskIndex + " " + highOutputTaskIndex + " " + numberOfOutputTasks + " " + channels[0]);
		
		return this.channels;
	}

}
