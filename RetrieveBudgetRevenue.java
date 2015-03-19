import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class RetrieveBudgetRevenue extends DBPedia {
	public static void Retrieve(String path, String path2) throws IOException										//Map-Reduce main class to retrieve budget, popularity and revenue of movies
	{
		JobConf conf = new JobConf(RetrieveBudgetRevenue.class);
		conf.setJobName("RetireveBudgetRevenue");
		FileInputFormat.addInputPath(conf,new Path(path));
		FileOutputFormat.setOutputPath(conf,new Path(path2));
		conf.setMapperClass(RevenueBudgetMapper.class);
		conf.setReducerClass(RevenueBudgetReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		JobClient.runJob(conf);
	}
}