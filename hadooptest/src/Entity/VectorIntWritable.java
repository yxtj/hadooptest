package Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class VectorIntWritable implements WritableComparable<VectorIntWritable> {
	private ArrayList<Integer> value = new ArrayList<Integer>();

	public VectorIntWritable() {
	}

	public VectorIntWritable(ArrayList<Integer> o) {
		setValue(o);
	}
	public VectorIntWritable(Integer[] o) {
		setValue(o);
	}
	public VectorIntWritable(int[] o) {
		setValue(o);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(value.size());
		for (Integer i : value) {
			out.writeInt(i.intValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int N = in.readInt();
		for (int i = 0; i < N; i++) {
			value.add(in.readInt());
		}
	}

	@Override
	public int compareTo(VectorIntWritable o) {
		int n = o.value.size();
		if (value.size() != n)
			return value.size() - o.value.size();
		for (int i = 0; i < n; i++) {
			if (value.get(i) != o.value.get(i))
				return value.get(i) - o.value.get(i);
		}
		return 0;
	}

	@Override
	public int hashCode() {
		int res = 0;
		for (int i : value) {
			res = (res << 2 | res >>> (32 - 2)) ^ i;
		}
		return res;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof VectorIntWritable))
			return false;
		VectorIntWritable r=(VectorIntWritable)o;
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

	public void setValue(ArrayList<Integer> value) {
		this.value = value;
	}
	public void setValue(Integer[] array) {
		for(Integer i : array){
			value.add(i);
		}
	}
	public void setValue(int[] array) {
		for(int i : array){
			value.add(i);
		}
	}
}
