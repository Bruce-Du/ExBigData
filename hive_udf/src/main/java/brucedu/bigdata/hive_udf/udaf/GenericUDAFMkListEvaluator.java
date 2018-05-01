package brucedu.bigdata.hive_udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.List;

public class GenericUDAFMkListEvaluator extends GenericUDAFEvaluator {
	private PrimitiveObjectInspector inputOI;
	private StandardListObjectInspector loi;
	private StandardListObjectInspector internalMergeOI;

	@Override
	public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
		super.init(m, parameters);
		if(m == Mode.PARTIAL1){
			inputOI = (PrimitiveObjectInspector) parameters[0];
			return ObjectInspectorFactory.getStandardListObjectInspector(
					(PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI)
			);
		}else{
			if(! (parameters[0] instanceof StandardListObjectInspector)){
				inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
				return (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
			}else{
				internalMergeOI.getListElementObjectInspector();
				loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
				return loi;
			}
		}
	}

	static class MkArrayAggregationBuffer implements AggregationBuffer{
		List<Object> container;
	}

	@Override
	public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
		reset(ret);
		return ret;
	}

	@Override
	public void reset(AggregationBuffer agg) throws HiveException {
		((MkArrayAggregationBuffer) agg).container = new ArrayList<Object>();
	}

	@Override
	public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
		assert (parameters.length == 1);
		Object p = parameters[0];

		if (p != null){
			MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
			putIntoList(p, myagg);
		}
	}

	private void putIntoList(Object p, MkArrayAggregationBuffer myagg) {
		Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
		myagg.container.add(pCopy);
	}

	@Override
	public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
		ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
		ret.addAll(myagg.container);
		return ret;
	}

	@Override
	public void merge(AggregationBuffer agg, Object partial) throws HiveException {
		MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
		ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
		for(Object i: partialResult){
			putIntoList(i, myagg);
		}
	}

	@Override
	public Object terminate(AggregationBuffer agg) throws HiveException {
		MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
		ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
		ret.addAll(myagg.container);
		return ret;
	}
}
