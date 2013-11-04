package us.marek.cascading.viewrates;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 
 * Reduce complex user agent strings into a small combinations of operating systems and browsers; we don't care
 * about a browser version or what version of ActiveX, Flash or other stuff people were using, just about the
 * OS-browser combination; compare to Scalding's terseness and clarity:
 *
 *  val cleanBrowsers = joined.map('userAgent -> 'ua2) {
 *   
 *   val os = List("Android", "Blackberry", "iOS", "Linux", "OS X", "Windows")
 *   val browser = List("Chrome", "Firefox", "Opera")
 *   val combos = os.permutations.toList.zip(browser).map(x => x._1.map(y => (y, x._2))).flatten.toSet ++
 *                Set(("SmartTV", "WebKit"), ("Windows", "MSIE"), ("OS X", "Safari"),
 *                    ("iOS", "Safari"), ("Windows", "Safari"))
 *   
 *   s: String => combos
 *                  .find((t: (String, String)) => s.contains(t._1) && s.contains(t._2))                
 *                  match {
 *                    case Some(x) => x.productIterator.mkString("/")
 *                    case None    => "Other"
 *                  }
 *   
 *  }
 *
 * 
 * @author Marek Kolodziej
 *
 */
public class UserAgentRenamer extends BaseOperation implements Function {

	final private static Set<Tuple2<String, String>> agents;
		
	static {
		
		agents = new HashSet<Tuple2<String, String>>();
		
		for (final String s1 : new String[] {"Android", "Blackberry", "iOS", "Linux", "OS X", "Windows"}) {
			for (final String s2 : new String[] {"Chrome", "Firefox", "Opera"}) {
				agents.add(new Tuple2<String, String>(s1, s2));
			}
		}

		for (Tuple2<String, String> t : Arrays.asList(new Tuple2[] {
				new Tuple2<String, String>("SmartTV", "WebKit"), new Tuple2<String, String>("Windows", "MSIE"),
				new Tuple2<String, String>("OS X", "Safari"), new Tuple2<String, String>("iOS", "Safari") })) {
			
			agents.add(t);
		}
    }
	
    public UserAgentRenamer(final Fields fieldDeclaration) {
		
		super(1, fieldDeclaration);
    }
	
	@SuppressWarnings("serial")
	public void operate(final FlowProcess flowProcess, final FunctionCall functionCall) {
		
		final TupleEntry argument = functionCall.getArguments();
		final String rawUA = argument.getString(0);
		final String cleanUA = getAgent(rawUA);
		final TupleEntryCollector collector = functionCall.getOutputCollector();
		collector.setFields(this.fieldDeclaration);
		
		functionCall.getOutputCollector().add(new Tuple() {
			{
				add(cleanUA);
			}
		});

	}
	
	private String getAgent(final String ua) {
		
		for (final Tuple2<String, String> t : agents) {
			
			if (ua.contains(t._1()) && ua.contains(t._2())) {
				
				return t._1() + "/" + t._2();
			}
		}
		
		return "Other";	
	}

}
