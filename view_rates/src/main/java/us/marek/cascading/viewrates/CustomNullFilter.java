package us.marek.cascading.viewrates;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * This adds a check for an empty string and a "-" string to the check for a null
 * 
 * @author Marek Kolodziej
 *
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class CustomNullFilter extends BaseOperation implements Filter {

	 public boolean isRemove(final FlowProcess flowProcess, final FilterCall filterCall) {
		 
		 for (final Object value : filterCall.getArguments().getTuple()) {
			 
	         if (value == null) {
	        	 
	    	      return true;
	    	      
	         } else if (value instanceof String) {
	        	 
	        	 final String str = (String) value;
	        	 
	        	 if (str == "" || str == "-") {
	        		 
	        		 return true;
	        	 }
	        	 
	         }
	         
	      }

	    return false;
	    }

}
