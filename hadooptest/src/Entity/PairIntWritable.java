package Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairIntWritable implements WritableComparable<PairIntWritable> {
	private int v1, v2;

	public PairIntWritable() {
		v1 = v2 = 0;
	}

	public PairIntWritable(int v1, int v2) {
		setValue(v1, v2);
	}

	public PairIntWritable(Integer v1, Integer v2) {
		setValue(v1, v2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(v1);
		out.writeInt(v2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		v1=in.readInt();
		v2=in.readInt();
	}

	@Override
	public int compareTo(PairIntWritable o) {
		return v1 != o.v1 ? v1 - o.v1 : v2 - o.v2;
	}

	@Override
	public int hashCode() {
		return (v1 << 3) ^ v2;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof PairIntWritable))
			return false;
		PairIntWritable r=(PairIntWritable)o;
		return v1==r.v1 && v2==r.v2;
	}

	@Override
	public String toString() {
		return String.valueOf(v1) + "," + String.valueOf(v2);
	}

	public void setValue(int v1, int v2) {
		this.v1 = v1;
		this.v2 = v2;
	}

	public void setValue(Integer v1, Integer v2) {
		this.v1 = v1.intValue();
		this.v2 = v2.intValue();
	}

	public int getV1() {
		return v1;
	}

	public int getV2() {
		return v2;
	}

}
