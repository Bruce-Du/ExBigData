package brucedu.bigdata.hive_udf;

import antlr.SemanticException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "collect", value = "_FUNC_(x) - Returns a list of objects. "+
"CAUTION will easily OOM on large data sets")
public class GenericUDAFCollect extends AbstractGenericUDAFResolver {
	static final Log LOG = LogFactory.getLog(GenericUDAFCollect.class.getName());

	public GenericUDAFCollect(){
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws UDFArgumentTypeException {
		if(parameters.length != 1){
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly one argument is expected.");
		}
		if(parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
			throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but"
			+ parameters[0].getTypeName() + " was passed as parameter 1.");
		}
		return new GenericUDAFMkListEvaluator();
	}
}
