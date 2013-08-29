package eu.stratosphere.pact.runtime.hierarchical;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class PactHierarchicalCombiningTask extends AbstractTask {

	private ChannelSelector<PactRecord> selector;
	private MutableReader<PactRecord> input = null;
	private RecordWriter<PactRecord> output = null;
	private final ConcurrentHashMap<Integer, PactRecord> collector = new ConcurrentHashMap<Integer, PactRecord> ();
	private final int RECORDS_BEFORE_EMIT = 1000000;
	private int recordCount = 0;
	
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
		//this.collector = new ConcurrentHashMap<Integer, PactRecord> ();

	}

	@Override
	public void invoke() throws Exception 
	{
		PactRecord record = new PactRecord (2);
		MutableReader<PactRecord> input = this.input;
		RecordWriter<PactRecord> output = this.output;
		
		while (input.next(record)) 
		{	
			Integer key = record.getField (0, PactInteger.class).getValue();
			if (!collector.containsKey(key))
				collector.put(key, record);
			if (recordCount % RECORDS_BEFORE_EMIT == 0) {
				for (PactRecord finalRecord: collector.values()) {
					output.emit (finalRecord);
				}
				collector.clear();
			}
			recordCount++;
			
		}
		for (PactRecord finalRecord: collector.values()) 
			output.emit (finalRecord);
	}

}
