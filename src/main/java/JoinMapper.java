import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	// private Text word = new Text();
	// private Text tuple = new Text();

	private Text joinKey = new Text();
	private Text tuple = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		//tuple.set(split[2]);
		joinKey.set(split[1]);
		tuple.set(split[2] + ";" + split[0] + ";" + split[1]);
		context.write(joinKey, tuple);
		// cannot parse - ignore
	}
}
