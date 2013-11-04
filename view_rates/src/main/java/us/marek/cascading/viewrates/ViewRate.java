package us.marek.cascading.viewrates;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Calculate view rate and format string; 
 * pretty "ceremonial" code but this isn't Scalding :)
 * 
 * @author Marek Kolodziej
 *
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class ViewRate extends BaseOperation implements Function {
	
	public ViewRate(final Fields fieldDeclaration) {
		
		super(1, fieldDeclaration);
    }

	public void operate(final FlowProcess flowProcess, final FunctionCall functionCall) {
		
		final TupleEntry argument = functionCall.getArguments();

        String result = null;
		
		try {
			
			Double rate = Double.parseDouble(argument.getString("viewCt")) / Double.parseDouble(argument.getString("impCt")) * 100;
			result = String.format("%.2f%%", rate);
			
		} catch (final Exception e) {}
		
		final TupleEntryCollector collector = functionCall.getOutputCollector();
		collector.setFields(new Fields("viewRate"));
		
		final Tuple tuple = new Tuple();
		tuple.add(result);
		collector.add(tuple);
	
	}
	
}
