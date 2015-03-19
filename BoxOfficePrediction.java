/*************************************** Configuration of TMDB API to fetch details of actors, directors, and producers of a movie given its name *****************************************************
 * Code to get Movie Details from theMovieDB
 * Gets Movie Name from command line.
 * Create a Directory of Movie Name
 * Converts it into a URL
 * Creates a connection to TMDB
 * fetches the list of basic details of all the movies with matching name as the given
 * Separates the records and take the first record, as it exactly matches the name inserted
 * Analyzes the record to get its TMDB movie id
 * Generates a query to fetch movie cast from TMDB API through the found id
 * Analyzes the output of the query to get a list of names of Actors, Directors, and Producers.
 * Create three file in the created directory namely "Actors.txt", "Producers.txt", and "Directors.txt"
 * Write the lists in the respective files 
 * Call the DBPediaBase.PermuteActors(String path) function.
 * NOTE: The code is working and includes the API-key. If required can be tested by providing Movie Name in the argument in run configuration.
 */

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.StringTokenizer;

public class BoxOfficePrediction {
	public static void main (String[] args) throws Exception
	{
		String movie=args[0];
		String move = args[0];
		for (int i=1;i<args.length;i++)
		{
			movie=movie+"+"+args[i];
			move = move + " " + args[i];							// Movie Name Conversion from the given form to URL acceptable form, e.g. Iron Man to Iron+Man
		}
		String URL_gen = "http://api.themoviedb.org/3/search/movie?api_key=aba********************78&query=";         //Creates generalized URL with missing Movie Name.
		String URL_fin = URL_gen + movie;              // Completes the URL by appending the generated movie name.
		String dir=args[0];
		for(int i=1;i<args.length;i++)
		{
			dir=dir+" "+args[i];
		}
		dir=dir.toUpperCase();
		new File(dir).mkdir();                 //Creates a directory with movie name in the project folder 
		System.out.println(URL_fin);
		URL TMDB = new URL(URL_fin);
        URLConnection connect = TMDB.openConnection();             //Creates a connection to TMDB
        BufferedReader in = new BufferedReader(new InputStreamReader(connect.getInputStream()));
        String inputLine = in.readLine(); 
        System.out.println(inputLine);
        StringTokenizer tokens = new StringTokenizer(inputLine, ",");             //Separates the records by delimiting with ','
        in.close();
        String imp_token=null;
        String x;
        while (tokens.hasMoreTokens())
        {
        	x=tokens.nextToken();
        	String y = '"' + "id" + '"'; 
        	if(x.contains(y))							// Check if the record contains the keyword "id"
        	{
        		imp_token=x;
        		break;										//Takes the id token of first record and breaks
        	}
        }
        System.out.println(imp_token);
        StringTokenizer token1=new StringTokenizer(imp_token,":");       //Further Tokenizes the id token to get only the value of id
        token1.nextToken();
        String id=token1.nextToken();
        System.out.println(id);
        String URL1_gen1 = "http://api.themoviedb.org/3/movie/";
        String URL1_gen2 = "/casts?api_key=aba********************78";
        String URL1_fin = URL1_gen1 + id + URL1_gen2;                 //Generates URL to fetch cast of the movie from the movie id through TMDB API
        URL TMDB1 = new URL(URL1_fin);
        URLConnection connect1 = TMDB1.openConnection();             //Creates a connection to TMDB
        BufferedReader in1 = new BufferedReader(new InputStreamReader(connect1.getInputStream()));
        String inputLine1;
        String name=null;
        while ((inputLine1 = in1.readLine()) != null) 
        {
        	name = name + " " + inputLine1;						//Creates the list of cast details returned by API
        }
        StringTokenizer tokens2=new StringTokenizer(name,"{");			//Tokenizes the list of cast to form records
        String Characters[];								//Array to store name of actors
        Characters = new String[100];
        String Directors[];									//Array to store name of Directors
        Directors = new String[20];
        String Producers[];									//Array to store name of Producers
        Producers = new String[20];
        int index1=0;
        int index2=0;
        int index3=0;
        while(tokens2.hasMoreTokens())
        {
        	String temp = tokens2.nextToken();
        	StringTokenizer temp1 = new StringTokenizer(temp,",");           //For every record, further tokenizes to separate details of a particular cast member
        	while (temp1.hasMoreTokens())
        	{
        		
        		String temp3 = temp1.nextToken();
        		if(temp3.contains("character"))								//Checks if the token contains the keyword character, it would means the cast member is an actor 
        		{
        			while(temp1.hasMoreTokens())
        			{
        				StringTokenizer temp4 = new StringTokenizer(temp1.nextToken(),":");				//Further tokenizes to get the name of the cast member
        				while(temp4.hasMoreTokens())
        				{
        					String temp5 = temp4.nextToken();
        					if(temp5.contains("name"))
        					{
        						String names=temp4.nextToken();
        						int index = names.lastIndexOf('"');
        						System.out.println(names);
        						
        						System.out.println(names.lastIndexOf('"'));
        						if(index!=0)
        							Characters[index1]=names.substring(1,index);						//Puts the name of the cast member into the Characters array 
        						else
        							Characters[index1]=names.substring(1,names.length());
        						index1++;
        					}
        				}
        			}
        		}
        		if(temp3.contains("Director") && !temp3.contains("Director of Photography"))			//Checks if the token contains the keyword Director and not Director of Photography, it would mean that the cast member is a Director
        		{
        			while(temp1.hasMoreTokens())
        			{
        				StringTokenizer temp4 = new StringTokenizer(temp1.nextToken(),":");				//Further tokenizes to get the name of the cast member
        				while(temp4.hasMoreTokens())
        				{
        					String temp5 = temp4.nextToken();
        					if(temp5.contains("name"))
        					{
        						String names=temp4.nextToken();
        						int index = names.lastIndexOf('"');
        						Directors[index2]=names.substring(1,index);								//Puts the name into the Directors array
        						index2++;
        					}
        				}
        			}
        		}
        		if(temp3.contains("Producer"))														//Checks if the token contains the keyword Producer, it means that the cast member is a Producer
        		{
        			while(temp1.hasMoreTokens())
        			{
        				StringTokenizer temp4 = new StringTokenizer(temp1.nextToken(),":");			//Further tokenizes to get the name of the cast member
        				while(temp4.hasMoreTokens())
        				{
        					String temp5 = temp4.nextToken();
        					if(temp5.contains("name"))
        					{
        						String names=temp4.nextToken();
        						int index = names.lastIndexOf('"');
        						Producers[index3]=names.substring(1,index);							//Puts the name into the Producers Array
        						index3++;
        					}
        				}
        			}
        		}
        	}
        }
        
        String file1 = dir+"/Actors.txt";					
        File file = new File(file1);										//Creates the Actors.txt file in the movie directory
        BufferedWriter output = new BufferedWriter(new FileWriter(file));
        System.out.println("\nActors:");
        for(int j=0;j<Characters.length;j++)
        {
        	if(Characters[j]!=null)
        		{
        			output.write(Characters[j]);							//Writes the names of Actors in the file, one name per line		
        			output.newLine();
        			System.out.println(Characters[j]);
        		}
        	else
        	{
        		break;
        	}
        }
        output.close();
        System.out.println("\n\nDirectors:");
        String file2 = dir+"/Directors.txt";						//Creates the Directors.txt file in the Directory
        File file3 = new File(file2);
        BufferedWriter output1 = new BufferedWriter(new FileWriter(file3));
        for(int j=0;j<Directors.length;j++)
        {
        	if(Directors[j]!=null)
        	{
        		output1.write(Directors[j]);						//Writes the names of Directors in the file, one name per line	
    			output1.newLine();
        		System.out.println(Directors[j]);
        	}
        	else
        	{
        		break;
        	}
        }
        output1.close();
        System.out.println("\n\nProducers:");
        String file4 = dir+"/Producers.txt";
        File file5 = new File(file4);								//Creates the Producers.txt file in the Directory
        BufferedWriter output2 = new BufferedWriter(new FileWriter(file5));
        for(int j=0;j<Producers.length;j++)
        {
        	if(Producers[j]!=null)
        	{
        		output2.write(Producers[j]);						//Writes the names of Producers in the file, one name per line
    			output2.newLine();
        		System.out.println(Producers[j]);
        	}
        	else
        	{
        		break;
        	}
        }
        output2.close();
        //
        String URLL = "http://api.themoviedb.org/3/movie/";
        String URLR = "?api_key=aba********************78";
        String URL_finn= URLL + id + URLR;
        URL TMDB2 = new URL(URL_finn);
        URLConnection connect2 = TMDB2.openConnection();             //Creates a connection to TMDB
        BufferedReader in2 = new BufferedReader(new InputStreamReader(connect2.getInputStream()));
        String inputLine2 = in2.readLine();
        System.out.println(inputLine2);
        StringTokenizer budge = new StringTokenizer(inputLine2,",");
        String x1=null;
        while(budge.hasMoreTokens())										
        {
        	String budge1 = budge.nextToken();
        	if(budge1.contains("budget"))
        	{
        		x1=budge1;
        		break;
        	}
        }
        String budget=null;
        String popularity=null;
        String RelDate=null;
        if(x1!=null)
        {
        	StringTokenizer money = new StringTokenizer(x1,":");
        	money.nextToken();
        	budget=money.nextToken();														// Retrieves the budget of the input movie
        }
        String x2=null;
        while(budge.hasMoreTokens())
        {
        	String budge1 = budge.nextToken();
        	if(budge1.contains("popularity"))
        	{
        		x2=budge1;
        		break;
        	}
        }
        String x3=null;
        while(budge.hasMoreTokens())
        {
        	String budge1 = budge.nextToken();
        	if(budge1.contains("release_date"))
        	{
        		x3=budge1;
        		break;
        	}
        }
        if(x3!=null)
        {
        	StringTokenizer date = new StringTokenizer(x3,":");
        	date.nextToken();
        	String rely1 = date.nextToken();
        	String rely2 = rely1.substring(1, rely1.length()-2);
        	StringTokenizer rely = new StringTokenizer(rely2,"-"); 
        	RelDate=rely.nextToken();																					//Retrieves Release Date of the input movie
        }
        if(x2!=null)
        {
        	StringTokenizer money = new StringTokenizer(x2,":");
        	money.nextToken();
        	popularity=money.nextToken();																				//Retrieves popularity/rating of the input movie
        }
        File file6 = new File(dir+"/Budget_Popularity.txt");
        BufferedWriter output3 = new BufferedWriter(new FileWriter(file6));
        if(budget!=null)
        {
        	if(budget.equals("0"))
        		output3.write("Budget:139084697");
        	else
        		output3.write("Budget:" + budget);
        	output3.newLine();
        }
        if(popularity!=null)
        {
        	output3.write("Popularity:" + popularity);
        	output3.newLine();
        }
        if(RelDate!=null)
        {
        	output3.write("Release Year:" + RelDate);
        	output3.newLine();
        }
        output3.close();
        DBPediaBase.PermuteActors(dir,move,RelDate);															//Calls DBPediaBase.java's PermuteActors function
    }
}