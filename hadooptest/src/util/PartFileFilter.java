package util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class PartFileFilter implements PathFilter {

	private static final String regex=".*part-\\d+$";
	
	@Override
	public boolean accept(Path path) {
		return path.getName().matches(regex);
	}

}
