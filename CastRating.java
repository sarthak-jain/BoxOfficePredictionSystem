import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class CastRating extends DBPedia{																		//Map-Reduce to retrieve Ratings of Cast 
	public static void Retrieve(String path, String path2) throws IOException
	{
		JobConf conf = new JobConf(CastRating.class);
		conf.setJobName("CastRating");
		FileInputFormat.addInputPath(conf,new Path(path));
		FileOutputFormat.setOutputPath(conf,new Path(path2));
		conf.setMapperClass(CastRatingMapper.class);
		conf.setReducerClass(CastRatingReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);
	}
}