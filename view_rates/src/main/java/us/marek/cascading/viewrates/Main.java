package us.marek.cascading.viewrates;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * This is the class driving the job - declaring and connecting source/sink taps, connecting pipes, etc.;
 * it depends on functors defined in other classes (e.g. RawLogParser, EventFilter, etc.);
 * The name "Main" is customary for Cascading, but one can define any name in the static call
 * AppProps.setApplicationJarClass()
 * 
 * @author Marek Kolodziej
 *
 */
public class Main {

	public static void main(final String[] args) {

		final String baseDir       = args[0];
		final String logInPath     = args[1];
		final String catInPath     = args[2];
		final String outPath       = args[3];
		final String logTrapPath   = args[4];
		final String catTrapPath   = args[5];
		final String joinTrapPath  = args[6];	
		final String viewTrapPath  = args[7]; 
		final String impTrapPath   = args[8];
		final String countTrapPath = args[9];
		final String dotPath       = args[10];

		final Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, Main.class);
		
		/* change to HadoopFlowConnector for production and (pseudo)distributed testing;
		   note that this will also require switching package imports for TextLine
		   and TextDelimited from cascading.scheme.local to cascading.scheme.hadoop,
		   and from FileTap to Hfs
		 */
		final LocalFlowConnector flowConnector = new LocalFlowConnector(properties);
		
		// name fields coming in from the log files
		final Fields logFields = new Fields("logOffset", "logLine");
		
		/* note that taps are generic according to the API, but the generic usage isn't explained in the Cascading
		   documentation (http://docs.cascading.org/cascading/2.2/userguide/htmlsingle), and such usage is nowhere
		   to be found on stackoverflow or other places; therefore, for the time being, I'm using raw types here
		 */
		final Tap logInTap = new FileTap(new TextLine(logFields), logInPath);

		/* we want to extract placement id, creative id, user agent, event code, but on the first iteration, we'll just look at
		   user agent-category breakdowns; however, we'll also need placement id's to join the placement id-category metadata
		*/
		final Fields logElems = new Fields("p", "userAgent", "eventCode");
		
		/* let's create a log pipe; Each applies to individual lines, and is therefore a map operation; contrast this with
		   Every, which applies to aggregates, and is therefore a reduce operation; here we see the power of Cascading;
		   we apply the log parser, followed by a null filter; instead of jamming all logic into one mapper to simplify
		   the execution plan like in the Java MapReduce API, we can separate the operations in the pipeline; this makes
		   for cleaner and more reusable code, since for instance the null filter could be applied to other jobs, or to
		   many pipelines within the same job; also note that this allows us to insert the null filter between the parser
		   and the user agent "renamer" (a piece of logic that collapses individual user agent strings into larger
		   groupings such as "OS X/Chrome" or "Linux/Firefox"
		 */
		Pipe logPipe = new Pipe("logLine");
		logPipe = new Each(logPipe, logFields, new RawLogParser(logElems), logElems);
		logPipe = new Each(logPipe, new CustomNullFilter());
		logPipe = new Each(logPipe, new Fields("userAgent"), new UserAgentRenamer(new Fields("cleanUA")), Fields.ALL);

		/* the category fields will apply to a new data source, namely the placement ID-category metadata that will be
		   joined with the log data to enrich the log data with 
		 */
		final Fields categoryFields = new Fields("placement", "category");

		// load placement ID-category metadata from a CSV source
		final Tap catInTap = new FileTap(new TextDelimited(categoryFields, ","), catInPath);
		final Pipe catPipe = new Pipe("catPipe");

		/* A HashJoin is similar to Hive's map-side join and Pig's replicated join; it distributes the small
		   dataset across the cluster, loads it in memory on each task JVM, and streams the big dataset
		   across each task; the small dataset is the second one; HashJoins expect one or more small datasets
		   to be fit into memory, and at most one large dataset to be streamed
		 */
		final Pipe joinPipe = new HashJoin(logPipe, new Fields("p"), catPipe, new Fields("placement"), new InnerJoin());
		
		/* all GrouBys will be based on user agent-category breakdowns; this named field grouping will 
		   also be important in renames, since relations being joined can't have the same name for 
		   the same field - so for example for viewPipe, we rename "cleanUA" to "cleanUAView" and
		   "category" to "categoryView"; we do the same for impPipe
		 */
		final Fields uaCat = new Fields("cleanUA", "category");
		
		/* we need to split processing into two steams: views and impressions; we will join
		   those later in order to summarize views, impressions and the view rate for each
		   user agent-category combination
		 */
		Pipe viewPipe = new Pipe("viewPipe", joinPipe);
		
		/* we filter the data to only keep view records (eventCode == "106"); this will allow
		   us to do a proper count; we will do the same for impressions
		 */
		viewPipe = new Each(viewPipe, Fields.ALL, new EventFilter(Fields.ALL, "view"));
		viewPipe = new Rename(viewPipe, uaCat, new Fields("cleanUAView", "categoryView"));
		
		// group by user agent and category, given the necessary renames
		viewPipe = new GroupBy(viewPipe, new Fields("cleanUAView", "categoryView"));
		
		/* generate view counts per user agent-category group; only keep the group and view count fields;
		   this will allow us to use the very efficient HashJoin for merging the view and impression streams
		   because the reduced data will be small enough to fit in memory; we don't care about other fields
		   so we can efficiently do what is technically a regular inner join but in practice is a self-join
		   of a relation that was broken into two for the count phase 
		 */
		viewPipe = new Every(viewPipe, new Count(new Fields("viewCt")), new Fields("cleanUAView", "categoryView", "viewCt"));    
		
		// we do the same for impressions that we did for views; not very DRY but it's not Scala :)
		Pipe impPipe = new Pipe("impPipe", joinPipe);
	    impPipe = new Each(impPipe, Fields.ALL, new EventFilter(Fields.ALL, "impression"));
	    impPipe = new Rename(impPipe, uaCat, new Fields("cleanUAImp", "categoryImp"));
		impPipe = new GroupBy(impPipe, new Fields("cleanUAImp", "categoryImp"));		
		impPipe = new Every(impPipe, new Count(new Fields("impCt")), new Fields("cleanUAImp", "categoryImp", "impCt"));      

		/* very little data after the Every/Count reduce phase - can do a HashJoin;
		   otherwise can use a CoGroup, etc.
		 */
		Pipe groupCt = new HashJoin(viewPipe, new Fields("cleanUAView", "categoryView"), 
				                    impPipe, new Fields("cleanUAImp", "categoryImp"),
				                    new InnerJoin());
		
		/* we only needed to double up on the user agents and categories to join
		   the pipes separated for the view and impression calculation; now let's
		   discard one of the redundant groups, e.g. the impression group, 
		   and retain the view and impression counts, as well as the view rates 
		*/
		groupCt = new Discard(groupCt, new Fields("cleanUAImp", "categoryImp"));
		
		// the HashJoin of views and impressions will allow us to apply the ViewRate functor
		groupCt = new Each(groupCt, Fields.ALL, new ViewRate(new Fields("viewRate")), Fields.ALL);

		/* instantiate traps for data that fails to go through the exisiting pipes;
		   this is very useful for debugging all the pipes that we desire to monitor, and
		   will help us fix the functors we apply to each pipe 
		 */
		final Tap logTrapTap   = new FileTap(new TextDelimited(), logTrapPath);
		final Tap catTrapTap   = new FileTap(new TextDelimited(), catTrapPath);
		final Tap joinTrapTap  = new FileTap(new TextDelimited(), joinTrapPath);
		final Tap viewTrapTap  = new FileTap(new TextDelimited(), viewTrapPath);
		final Tap impTrapTap   = new FileTap(new TextDelimited(), impTrapPath);
		final Tap countTrapTap = new FileTap(new TextDelimited(), countTrapPath);		

		// we'll be emitting these fields int the final output
		final Fields outFields = new Fields("cleanUAView", "categoryView", "viewCt", "impCt", "viewRate");
		
		// tab-delimited output tap
		final Tap outTap = new FileTap(new TextDelimited(outFields, "\t"), outPath);
		
		// output pipe will be terminated by the output tap
		final Pipe outPipe = new Pipe("outPipe", groupCt);

		// define flow
		final FlowDef flowDef = FlowDef.flowDef().addSource(logPipe, logInTap)
				.addTrap(logPipe, logTrapTap)
				.addSource(catPipe, catInTap)
				.addTrap(catPipe, catTrapTap)
				.addTrap(joinPipe, joinTrapTap)
				.addTrap(groupCt, countTrapTap)
				.addTailSink(outPipe, outTap);    

		// connect flow to a flow connector (LocalFlowConnector or HadoopFlowConnector)
		final Flow flow = flowConnector.connect(flowDef);
		
		/* create a DOT file (Graphviz, Omnigraffle, Visio) to show the execution plan;
		   very useful for debugging and conceptualizing complex workflows
		 */
		flow.writeDOT(dotPath);
		
		// run job
		flow.complete();
	}
}