package Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TripleIntWritable implements WritableComparable<TripleIntWritable> {
	private int v1, v2, v3;

	public TripleIntWritable() {
		v1 = v2 = 0;
	}

	public TripleIntWritable(int v1, int v2, int v3) {
		setValue(v1, v2, v3);
	}

	public TripleIntWritable(Integer v1, Integer v2, Integer v3) {
		setValue(v1, v2, v3);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(v1);
		out.writeInt(v2);
		out.writeInt(v3);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		v1 = in.readInt();
		v2 = in.readInt();
		v3 = in.readInt();
	}

	@Override
	public int compareTo(TripleIntWritable o) {
		return v1 != o.v1 ? v1 - o.v1 : (v2 != o.v2 ? v2 - o.v2 : v3 - o.v3);
	}

	@Override
	public int hashCode() {
		int t = (v1 << 3) ^ v2;
		return (t << 3) ^ v3;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof TripleIntWritable))
			return false;
		TripleIntWritable r = (TripleIntWritable) o;
		return v1 == r.v1 && v2 == r.v2 && v3 == r.v3;
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(v1);
		buf.append(',');
		buf.append(v2);
		buf.append(',');
		buf.append(v3);
		return buf.toString();
	}

	public void setValue(int v1, int v2, int v3) {
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
	}

	public void setValue(Integer v1, Integer v2, Integer v3) {
		this.v1 = v1.intValue();
		this.v2 = v2.intValue();
		this.v3 = v3.intValue();
	}

	public int getV1() {
		return v1;
	}

	public int getV2() {
		return v2;
	}

	public int getV3() {
		return v3;
	}

}
