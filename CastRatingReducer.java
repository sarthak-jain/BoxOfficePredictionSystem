import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CastRatingReducer extends MapReduceBase										//Reducer class to retrieve cast rating
implements Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterator<Text> values,
OutputCollector<Text, Text> output, Reporter reporter)
throws IOException {
	Text val=null;
	while(values.hasNext())
		val=values.next();																	//Ignores all the values except last, thus stores only one result per cast member
	output.collect(key, val);
}
}
