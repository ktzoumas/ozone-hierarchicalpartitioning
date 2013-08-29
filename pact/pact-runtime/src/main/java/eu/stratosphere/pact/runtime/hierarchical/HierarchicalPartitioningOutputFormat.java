package eu.stratosphere.pact.runtime.hierarchical;

import java.io.IOException;

import com.google.common.base.Charsets;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class HierarchicalPartitioningOutputFormat extends FileOutputFormat {

	private final StringBuilder buffer = new StringBuilder();

	
	@Override
	public void writeRecord(PactRecord record) throws IOException 
	{
		buffer.setLength (0);
		
		buffer.append (record.getField(0, PactInteger.class).toString());
		buffer.append ('\t');
		buffer.append (record.getField (1, PactString.class).toString());
		buffer.append ('\n');

	    byte[] bytes = buffer.toString().getBytes(Charsets.UTF_8);
	    stream.write(bytes);
	}

}
