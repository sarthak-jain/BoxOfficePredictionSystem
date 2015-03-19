import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RevenueBudgetReducer extends MapReduceBase									//Reducer Class to retrieve Budget, Revenue, Popularity and Release-Year of the Movies
implements Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterator<Text> values,
OutputCollector<Text, Text> output, Reporter reporter)
throws IOException 
	{
		Text value = null;
		while(values.hasNext())
		{
			value=values.next();														//Ignores all values except last value, thus stores only one row per movie
		}
		output.collect(key, value);
	}
}
