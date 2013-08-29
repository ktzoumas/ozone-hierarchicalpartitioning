package eu.stratosphere.pact.runtime.hierarchical;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class GenerationAndPartitioningTask extends AbstractGenericInputTask {

	
	private RecordWriter<PactRecord> output;
	private ChannelSelector<PactRecord> selector;
	
	private String payload = "Slowly slowly started the slug for the mountain again and again and again...";
	
	final long a = 0xffffda61L;
	long seed;
	
	@Override
	public void registerInputOutput() 
	{
		boolean isHierarchicalPartitioning = this.getTaskConfiguration().getBoolean("isHierarchicalPartitioning", false);
		if (isHierarchicalPartitioning) {
			int numberOfTeams = this.getTaskConfiguration().getInteger("numberOfTeams", 0);	
			int indexInSubTaskGroup = this.getIndexInSubtaskGroup();		
			int numberOfSubTasks = this.getTaskConfiguration().getInteger("degreeOfParallelism", -1);
			int numberOfSubTasksPerTeam = numberOfSubTasks / numberOfTeams;
			int teamIndex = indexInSubTaskGroup / numberOfSubTasksPerTeam;
			int low = teamIndex * numberOfSubTasksPerTeam;
			int high = (teamIndex + 1) * numberOfSubTasksPerTeam;
			
			this.selector = new PactHierarchicalHashChannelSelector (low, high, numberOfSubTasks);
		}
		else {
			this.selector = new PactHashChannelSelector ();
		}
		
		this.output = new RecordWriter<PactRecord> (
				this, 
				PactRecord.class, 
				selector
				);
		
	}

	@Override
	public void invoke() throws Exception 
	{
		int numberOfRecordsToEmit = 
				this.getTaskConfiguration().getInteger("generatedRecordsPerTask", 10);
		
		int maxKeyValue = 
				this.getTaskConfiguration().getInteger("maxKeyValue", 1024);
	
		//int payloadSize =
		//		this.getTaskConfiguration().getInteger("payloadSize", 128);
		
		PactRecord record = new PactRecord(2);
		record.setField (1, new PactString (payload));
		RecordWriter<PactRecord> output = this.output;
		PactInteger key = new PactInteger ();
		
		seed = System.nanoTime() & 0xffffffffL;
		
		try {
			//Random rand = new Random ();
			
			for (int i = 0; i < numberOfRecordsToEmit; i++) {
				//int r = rand.nextInt(Integer.MAX_VALUE);
				
				int r = (int) ((a * (seed & 0xffffffffL)) + (seed >>> 32));				
				seed = r;
				
				int keyValue = r % maxKeyValue;
				if (keyValue < 0 )
					keyValue = -keyValue;
				
				//record.clear();
				//record.addField (new PactInteger (keyValue));
				key.setValue(keyValue);
				record.setField (0, key);
				//record.addField (new PactString (payload) );
				output.emit (record);
			}

		}
		catch (Exception e) {
			System.err.println("Error in producer: " + e.toString());
			e.printStackTrace();
			System.exit(1);
		}
		finally {
			//if (writer != null) 
			//	writer.close();
		}	
		
	}
}
