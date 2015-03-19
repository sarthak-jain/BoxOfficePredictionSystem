import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CastRatingMapper extends MapReduceBase																//Mapper Class to Retrieve Cast Ratings 
implements Mapper<LongWritable, Text, Text, Text>  {
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		try{
			Integer.parseInt(value.toString());
		}
		catch (Exception e)
		{
		StringTokenizer tokens = new StringTokenizer(value.toString(),"(");
		String actor = null;
		while(tokens.hasMoreTokens())
		{
			actor = tokens.nextToken();
			String newact = actor.substring(0, actor.length()-2);
			StringTokenizer actors = new StringTokenizer(newact,",");
			while(actors.hasMoreTokens())
			{
				String x=null;
				StringTokenizer token = new StringTokenizer(actors.nextToken(),"_");
				while(token.hasMoreTokens())
				{
					if(x==null)
					{
						x=token.nextToken();
					}
					else
					{
						x=x+"+"+token.nextToken();
					}
				}
				String URL_fin="http://api.themoviedb.org/3/search/person?api_key=aba********************78&query=" + x;
				URL TMDB = new URL(URL_fin);
				URLConnection connect = TMDB.openConnection();
				BufferedReader in = new BufferedReader(new InputStreamReader(connect.getInputStream()));										//TMDB Query execution to retrieve cast information
		        String inputLine = in.readLine();
		        StringTokenizer newtoken = new StringTokenizer(inputLine,",");
		        String popularity=null;
		        while(newtoken.hasMoreTokens())
		        {
		        	String tokenw=newtoken.nextToken();
		        	if(tokenw.contains("popularity"))
		        	{
		        		StringTokenizer newtoken1=new StringTokenizer(tokenw,":");
		        		while(newtoken1.hasMoreTokens())
		        		{
		        			newtoken1.nextToken();
		        			popularity = newtoken1.nextToken();																			//Extracts popularity/rating of a cast member
		        		}
		        	}
		        }
		        if(popularity!=null)
		        	output.collect(new Text(x), new Text(popularity));
		    }
			if(tokens.hasMoreTokens())
				tokens.nextToken();
		}
	}
	}
}