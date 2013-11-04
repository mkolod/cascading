package us.marek.cascading.viewrates;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Filter for specific events, e.g. "view" and "impression"
 * 
 * @author Marek Kolodziej
 *
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class EventFilter extends BaseOperation implements Filter {
	
	final String event;
	
	public EventFilter(final Fields fieldDeclaration, final String event) {
		
		super(fieldDeclaration);
		this.event = event;
	}
	
	public boolean isRemove(final FlowProcess flowProcess, final FilterCall filterCall) {
		
		final TupleEntry entry = filterCall.getArguments();
		
		final String s = (String) entry.getObject("eventCode");
		
		return s == null || !s.equals(event);
	}

}
