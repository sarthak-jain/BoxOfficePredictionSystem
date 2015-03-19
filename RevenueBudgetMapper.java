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

public class RevenueBudgetMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>  {
	int RelDate=0;
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			try
			{
				RelDate = Integer.parseInt(value.toString());
			}
			catch (Exception e)
			{
			StringTokenizer tokens = new StringTokenizer(value.toString(),"(");
			String movie = null;
			//String actors = null;
			String valuer=null;
			while(tokens.hasMoreTokens())
			{
				tokens.nextToken();
				String name1=tokens.nextToken();
				StringTokenizer token2 = new StringTokenizer(name1,")");
				String movie1 = token2.nextToken();
				StringTokenizer tokens3 = new StringTokenizer(movie1);
				while(tokens3.hasMoreTokens())
				{
					if(movie==null)
					{
						movie=tokens3.nextToken();
					}
					else
					{
						movie=movie + "+" + tokens3.nextToken();
					}
				}
				String URL_gen = "http://api.themoviedb.org/3/search/movie?api_key=aba********************78&query=";
				String URL_fin = URL_gen + movie;
				URL TMDB = new URL(URL_fin);
		        URLConnection connect = TMDB.openConnection();
		        BufferedReader in = new BufferedReader(new InputStreamReader(connect.getInputStream()));					//TMDB query execution to retrieve movie information
		        String inputLine = in.readLine();
		        StringTokenizer tokens5 = new StringTokenizer(inputLine, ",");             //Separates the records by delimiting with ','
		        in.close();
		        String imp_token=null;
		        String imp_token1=null;
		        String imp_token2=null;
		        String x;
		        while (tokens5.hasMoreTokens())
		        {
		        	x=tokens5.nextToken();
		        	String y = '"' + "id" + '"'; 
		        	if(x.contains(y))							// Check if the record contains the keyword "id"
		        	{
		        		imp_token=x;
		        		break;										//Takes the id token of first record and breaks
		        	}
		        }
		        String k;
		        while (tokens5.hasMoreTokens())
		        {
		        	k=tokens5.nextToken();
		        	if(k.contains("release_date"))
		        	{
		        		imp_token1=k;
		        		break;
		        	}
		        }
		        String l;
		        while(tokens5.hasMoreTokens())
		        {
		        	l=tokens5.nextToken();
		        	if(l.contains("popularity"))
		        	{
		        		imp_token2=l;
		        		break;
		        	}
		        }
		        if(imp_token!=null)
		        {
		        	StringTokenizer token1=new StringTokenizer(imp_token,":");       //Further Tokenizes the id token to get only the value of id
			        token1.nextToken();
			        String id=token1.nextToken();
			        String URL1_gen1 = "http://api.themoviedb.org/3/movie/";
			        String URL1_gen2 = "?api_key=aba********************78";
			        String URL1_fin = URL1_gen1 + id + URL1_gen2;                 //Generates URL to fetch cast of the movie from the movie id through TMDB API
			        URL TMDB1 = new URL(URL1_fin);
			        URLConnection connect1 = TMDB1.openConnection();             //Creates a connection to TMDB
			        BufferedReader in1 = new BufferedReader(new InputStreamReader(connect1.getInputStream()));
			        String inputLine1 = in1.readLine();
			        StringTokenizer tokens8 = new StringTokenizer(inputLine1,",");
			        String budget = null;
			        String budget_movie=null;
				    String revenue_movie=null;
			        while(tokens8.hasMoreTokens())
			        {
			        	String next = tokens8.nextToken();
			        	if(next.contains("budget"))
			        	{
			        		budget = next;
			        		//System.out.println(budget);
			        		break;
			        	}
			        }
				    String revenue=null;
				    while(tokens8.hasMoreTokens())
				    {
				    	String next = tokens8.nextToken();
				    	if(next.contains("revenue"))
			        	{
			        		revenue = next;
			        		//System.out.println(revenue);
			        		break;
			        	}
				    }
				    
			        if(budget!=null)
		        	{
		        		StringTokenizer token9 = new StringTokenizer(budget,":");
		        		token9.nextToken();
		        		budget_movie=token9.nextToken();
		        	}
			        if(revenue!=null)
			        {
			        	StringTokenizer token9 = new StringTokenizer(revenue,":");
		        		token9.nextToken();
		        		revenue_movie=token9.nextToken();
			        }
			        if(budget_movie!=null&&revenue_movie!=null)
			        {
			        	if((!(budget_movie.toString().equals("0")))&&(!revenue_movie.toString().equals("0")))
			        	{
			        		String release_year=null;
					        if(imp_token1!=null)
					        {
					        	StringTokenizer token13 = new StringTokenizer(imp_token1,":");
					        	token13.nextToken();
					        	String release_year1=token13.nextToken();
					        	StringTokenizer rel = new StringTokenizer(release_year1.substring(1, release_year1.length()-2),"-");
					        	String rel1=rel.nextToken();
					        	if(Integer.parseInt(rel1)<RelDate||RelDate==0)
					        		release_year=rel1;														//Retrieves release-year of the movie 
					        }
					        String popularity=null;
					        if(imp_token2!=null)
					        {
					        	StringTokenizer token13 = new StringTokenizer(imp_token2,":");
					        	token13.nextToken();
					        	popularity=token13.nextToken();
					        }
					        if(release_year!=null&&popularity!=null)
			        		{
					        	try{
					        		float rev = Integer.parseInt(revenue_movie);
					        		float bud = Integer.parseInt(budget_movie);
					        		valuer = (rev/bud) + " " + release_year + " " + popularity;
						        	output.collect(new Text(movie), new Text(valuer));
					        	}
					        	catch (Exception er)
					        	{
					        		valuer = budget_movie.toString() + " " +revenue_movie.toString() + " " + release_year + " " + popularity;
						        	output.collect(new Text(movie), new Text(valuer));
					        	}
			        		}
			        	}
			        }
			    }
			}
		}
	}
}
