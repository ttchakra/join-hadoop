import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer<KEY> extends Reducer<KEY, Text, KEY, Text> {

	// private Text result = new Text();

	private Text result = new Text();
	String checkTable1 = null;
	String checkTable2 = null;

	public void reduce(KEY key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String temp = "";

		for (Text val_r : values) {
			String[] split1 = val_r.toString().split(";");
			checkTable1 = split1[1];
			for (Text val_s : values) {
				String[] split2 = val_s.toString().split(";");
				checkTable2 = split2[1];

				//System.out.println("Temps: " + temp);

				//System.out.println("SP: " + split2[0] + " - " + split2[1] + ". " + split1[0] + " - " + split1[0]);

				if (!checkTable1.equals(checkTable2)) {
					String temp1 = (split1[0]+","+split2[0]);
					//System.out.println("Temp1: " + temp1);
					temp += (temp1);
					
				}
			}

		}
		result.set(temp);

		context.write(null, result);
	}

}