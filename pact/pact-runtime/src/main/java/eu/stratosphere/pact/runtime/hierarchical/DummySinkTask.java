package eu.stratosphere.pact.runtime.hierarchical;

import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.pact.common.type.PactRecord;

public class DummySinkTask extends AbstractOutputTask {

	private MutableReader<PactRecord> input;
	
	@Override
	public void registerInputOutput() 
	{
		this.input = new MutableRecordReader<PactRecord>(this);
	}

	@Override
	public void invoke() throws Exception 
	{	
		PactRecord rec = new PactRecord();
		MutableReader<PactRecord> input = this.input;
		//return;
		while (input.next(rec)) {
			//input.next();
		}
	}

}
