package Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class VectorDoubleWritable implements WritableComparable<VectorDoubleWritable> {
	private ArrayList<Double> value = new ArrayList<Double>();

	public VectorDoubleWritable() {
	}

	public VectorDoubleWritable(ArrayList<Double> o) {
		setValue(o);
	}
	public VectorDoubleWritable(Double[] o) {
		setValue(o);
	}
	public VectorDoubleWritable(double[] o) {
		setValue(o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(value.size());
		for (Double i : value) {
			out.writeDouble(i.doubleValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		double N = in.readDouble();
		for (double i = 0; i < N; i++) {
			value.add(in.readDouble());
		}
	}

	@Override
	public int compareTo(VectorDoubleWritable o) {
		double n = o.value.size();
		if (value.size() != n)
			return value.size() - o.value.size();
		for (int i = 0; i < n; i++) {
			if (value.get(i) != o.value.get(i))
				return (value.get(i) - o.value.get(i))<0.0?-1:1;
		}
		return 0;
	}

	@Override
	public int hashCode() {
		int res = 0;
		for (double i : value) {
			long t=Double.doubleToLongBits(i);
			res = (res << 3 | res >>> (32 - 3)) ^ (int)(t^t>>>32);
		}
		return res;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof VectorDoubleWritable))
			return false;
		VectorDoubleWritable r=(VectorDoubleWritable)o;
		if(value.size()!=r.value.size())
			return false;
		for(int i=0;i<value.size();i++)
			if(value.get(i)!=r.value.get(i))
				return false;
		return true;
	}

	@Override
	public String toString() {
		return value.toString().replaceAll("[\\[\\] ]", "");
	}

	public void setValue(ArrayList<Double> value) {
		this.value = value;
	}
	public void setValue(Double[] array) {
		for(Double i : array){
			value.add(i);
		}
	}
	public void setValue(double[] array) {
		for(double i : array){
			value.add(i);
		}
	}
}
