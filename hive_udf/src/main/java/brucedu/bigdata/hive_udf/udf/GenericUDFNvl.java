package brucedu.bigdata.hive_udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class GenericUDFNvl extends GenericUDF {
	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnIOResolover;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		argumentOIs = arguments;
		if(arguments.length != 2){
			throw new UDFArgumentLengthException("The operator 'NVL' accepts 2 arguments.");
		}
		returnIOResolover = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		if(!(returnIOResolover.update(arguments[0]) && returnIOResolover.update(arguments[1]))){
			throw new UDFArgumentTypeException(2,
					"The 1st and 2nd args of function NLV should have the same type, "
							+ "but they are different: \"" + arguments[0].getTypeName()
							+ "\" and \"" + arguments[1].getTypeName() + "\"");
		}
		return returnIOResolover.get();
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object retVal = returnIOResolover.convertIfNecessary(arguments[0].get(),
				argumentOIs[0]);
		if(retVal == null){
			retVal = returnIOResolover.convertIfNecessary(arguments[1].get(),
					argumentOIs[1]);
		}
		return retVal;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("if ");
		sb.append(children[0]);
		sb.append(" is null ");
		sb.append("returns");
		sb.append(children[1]);
		return sb.toString();
	}
}
