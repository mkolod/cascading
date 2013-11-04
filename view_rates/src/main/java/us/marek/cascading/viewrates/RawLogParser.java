package us.marek.cascading.viewrates;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import java.util.HashMap;
import java.util.Map;


/**
 * This is based on the original Pig raw log parser
 * 
 * @author Marek Kolodziej
 * 
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class RawLogParser extends BaseOperation implements Function {

	private final String[] fields;

	public RawLogParser(final Fields fieldDeclaration) {

		super(1, fieldDeclaration);

		final Fields decl = this.fieldDeclaration;

		fields = new String[decl.size()];

		for (int i = 0; i < decl.size(); i++) {

			fields[i] = decl.get(i).toString();
		}
	}

	@SuppressWarnings("serial")
	public void operate(final FlowProcess flowProcess, final FunctionCall functionCall) {

		final TupleEntry argument = functionCall.getArguments();

		final Map<String, String> map = parse(argument.getString(1));

		final TupleEntryCollector collector = functionCall.getOutputCollector();

		collector.setFields(new Fields(fields));

		if (!map.isEmpty()) {

			functionCall.getOutputCollector().add(new Tuple() {
				{
					for (final String s : fields) {

						add(map.get(s));
					}
				}
			});

		}
	}

	
	private Map<String, String> parse(final String input) {
		
		final String[] elems = input.split(",");
		
		return new HashMap<String, String>() {
			{
				
				for (final String elem : elems) {
					
					final String[] keyVal = elem.split("=");
					put(keyVal[0], keyVal[1]);
				}
			}
		};
	}

}
