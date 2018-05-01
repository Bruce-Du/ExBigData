package brucedu.bigdata.hive_udf;

import javafx.beans.binding.ListBinding;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

/**
 * Hive编程指南 13.10.1
 * 产生多行数据的UDTF
 */
public class GenericUDTFFor extends GenericUDTF {
	private IntWritable start;
	private IntWritable end;
	private IntWritable inc;

	private Object[] forwardObj = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args){
		start = ((WritableConstantIntObjectInspector) args[0]).getWritableConstantValue();
		end = ((WritableConstantIntObjectInspector) args[1]).getWritableConstantValue();
		if(args.length == 3){
			inc = ((WritableConstantIntObjectInspector) args[2]).getWritableConstantValue();
		}else{
			inc = new IntWritable(1);
		}

		this.forwardObj = new Object[1];
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		fieldNames.add("col0");
		fieldOIs.add(
				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT)
		);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		for(int i = start.get(); i < end.get(); i += inc.get()){
			this.forwardObj[0] = new Integer(i);
			forward(forwardObj);
		}
	}

	@Override
	public void close() throws HiveException {
	}
}
