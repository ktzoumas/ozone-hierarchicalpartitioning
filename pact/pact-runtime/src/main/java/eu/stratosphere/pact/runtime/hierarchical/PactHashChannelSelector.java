package eu.stratosphere.pact.runtime.hierarchical;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class PactHashChannelSelector implements ChannelSelector<PactRecord>{

	private int [] channels = {0};
	
	@Override
	public int[] selectChannels(PactRecord record, int numberOfOutputChannels) {
		
		PactInteger hashInt = record.getField(0, PactInteger.class);
		
		int hash = hashInt.getValue();
		
		channels[0] = (hash < 0) 
				? -hash % numberOfOutputChannels 
						: hash % numberOfOutputChannels;
		
		return this.channels;
		
	}

}
