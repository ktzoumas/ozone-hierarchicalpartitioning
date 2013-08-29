package eu.stratosphere.pact.runtime.hierarchical;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.PactRecord;

public class PactHierarchicalPartitioningTask extends AbstractTask 
{
	
	private ChannelSelector<PactRecord> selector;
	private MutableReader<PactRecord> input = null;
	private RecordWriter<PactRecord> output = null;

	@Override
	public void registerInputOutput() 
	{
		int numberOfTeams = this.getTaskConfiguration().getInteger("numberOfTeams", 0);	
		int indexInSubTaskGroup = this.getIndexInSubtaskGroup();		
		int numberOfSubTasks = this.getTaskConfiguration().getInteger("degreeOfParallelism", -1);
		int numberOfSubTasksPerTeam = numberOfSubTasks / numberOfTeams;
		int teamIndex = indexInSubTaskGroup / numberOfSubTasksPerTeam;
		int low = teamIndex * numberOfSubTasksPerTeam;
		int high = (teamIndex + 1) * numberOfSubTasksPerTeam;
		
		//this.selector = new TeamBasedChannelSelector (low, high);
		
		
		this.selector = new PactHierarchicalHashChannelSelector (low, high, numberOfSubTasks);
		this.input = new MutableRecordReader<PactRecord> (this);
		this.output = new RecordWriter<PactRecord> (
					this, 
					PactRecord.class, 
					selector);
		
	}

	@Override
	public void invoke() throws Exception 
	{
		PactRecord record = new PactRecord (2);
		MutableReader<PactRecord> input = this.input;
		RecordWriter<PactRecord> output = this.output;
		
		while (input.next(record)) {				
			output.emit (record);
		}
	}
	

}
