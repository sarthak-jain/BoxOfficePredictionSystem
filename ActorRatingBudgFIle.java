/* This file looks for the ActorPermute, DirectorPermute, ProducerPermute, ActorDirectorPermute, ActorProducerPermute and DirectorProducerPermute Movies in the respective files, and retrieves their cast
 * rating information from the respective files made by the map-reduce tasks.
 * It then takes the average of the ratings and performs operations on it with the popularity of the movie and the ratio of revenue and budget of the movie.
 */
import java.io.*;
import java.util.StringTokenizer;

public class ActorRatingBudgFIle extends DBPedia {
		public static void Act(String path) throws Exception
		{
			BufferedReader in1 = new BufferedReader(new FileReader(path+"/output/part-00000"));
			String inputLine=null;
			File file = new File(path+"/final_Actor.txt");
			BufferedWriter output = new BufferedWriter(new FileWriter(file));
			while((inputLine = in1.readLine())!=null)
			{
				StringTokenizer newer = new StringTokenizer(inputLine);
				String movie_name = newer.nextToken();
				StringTokenizer newer1 = new StringTokenizer(movie_name,"+");
				String movie = null;
				while(newer1.hasMoreTokens())
				{
					if(movie==null)
					{
						movie = newer1.nextToken();
					}
					else
					{
						movie = movie + " " +newer1.nextToken();
					}
				}
				BufferedReader in3 = new BufferedReader(new FileReader(path+"/ActorPermuteMovies.txt"));
				String inputLine2=null;
				in3.readLine();
				while((inputLine2 = in3.readLine())!=null)
				{
					StringTokenizer newer2 = new StringTokenizer(inputLine2,"(");
					String move_act = newer2.nextToken();
					String movie1=newer2.nextToken();
					if(movie1.substring(0, movie1.length()-1).equals(movie))
					{
						StringTokenizer newer3= new StringTokenizer(move_act,",");
						String Act1=newer3.nextToken();
						String Act2=newer3.nextToken();
						Act2 = Act2.substring(0, Act2.length()-2);
						Act1=Act1.replace('_', '+');
						Act2 = Act2.replace('_', '+');
						BufferedReader in2 = new BufferedReader(new FileReader(path+"/output6/part-00000"));
						String inputLine3 = null;
						Float Act1R=0f,Act2R=0f;
						while((inputLine3=in2.readLine())!=null)
						{
							StringTokenizer Actors = new StringTokenizer(inputLine3);
							String Act3 = Actors.nextToken();
							String Rate = Actors.nextToken();
							if(Act3.equals(Act1))
							{
								Act1R = Float.parseFloat(Rate);
							}
							if(Act3.equals(Act2))
							{
								Act2R = Float.parseFloat(Rate);
							}
							if(!Act1.contains(Act2))
							{
								Act1=Act1+Act2;
								Act1R=Act1R+Act2R;
							}
						}
						StringTokenizer count = new StringTokenizer(Act1);
						Float average = Act1R/count.countTokens();
						Float ratio = 1f;
						Float popularity = 1f;
						if(newer.hasMoreTokens())
							ratio = Float.parseFloat(newer.nextToken());
						if(newer.hasMoreTokens())
							newer.nextToken();
						if(newer.hasMoreTokens())
							popularity = Float.parseFloat(newer.nextToken());
						Float out=average*ratio*popularity;
						output.write(out.toString());
						output.newLine();
						in2.close();
					}
				}
				in3.close();
			}
			in1.close();
			output.close();
			BufferedReader in4 = new BufferedReader(new FileReader(path+"/output1/part-00000"));
			String inputLine4=null;
			File file4 = new File(path+"/final_Director.txt");
			BufferedWriter output4 = new BufferedWriter(new FileWriter(file4));
			while((inputLine4 = in4.readLine())!=null)
			{
				StringTokenizer newer = new StringTokenizer(inputLine4);
				String movie_name = newer.nextToken();
				StringTokenizer newer1 = new StringTokenizer(movie_name,"+");
				String movie = null;
				while(newer1.hasMoreTokens())
				{
					if(movie==null)
					{
						movie = newer1.nextToken();
					}
					else
					{
						movie = movie + " " +newer1.nextToken();
					}
				}
				BufferedReader in3 = new BufferedReader(new FileReader(path+"/DirectorPermuteMovies.txt"));
				String inputLine2=null;
				in3.readLine();
				while((inputLine2 = in3.readLine())!=null)
				{
					StringTokenizer newer2 = new StringTokenizer(inputLine2,"(");
					String move_act = newer2.nextToken();
					String movie1=newer2.nextToken();
					if(movie1.substring(0, movie1.length()-1).equals(movie))
					{
						StringTokenizer newer3= new StringTokenizer(move_act,",");
						String Act1=newer3.nextToken();
						String Act2=newer3.nextToken();
						Act2 = Act2.substring(0, Act2.length()-2);
						Act1=Act1.replace('_', '+');
						Act2 = Act2.replace('_', '+');
						BufferedReader in2 = new BufferedReader(new FileReader(path+"/output7/part-00000"));
						String inputLine3 = null;
						Float Act1R=0f,Act2R=0f;
						while((inputLine3=in2.readLine())!=null)
						{
							StringTokenizer Actors = new StringTokenizer(inputLine3);
							String Act3 = Actors.nextToken();
							String Rate = Actors.nextToken();
							if(Act3.equals(Act1))
							{
								Act1R = Float.parseFloat(Rate);
							}
							if(Act3.equals(Act2))
							{
								Act2R = Float.parseFloat(Rate);
							}
							
							if(!Act1.contains(Act2))
							{
								Act1=Act1+Act2;
								Act1R=Act1R+Act2R;
							}
						}
						StringTokenizer count = new StringTokenizer(Act1);
						Float average = Act1R/count.countTokens();
						Float ratio = 1f;
						Float popularity = 1f;
						if(newer.hasMoreTokens())	
							ratio = Float.parseFloat(newer.nextToken());
						if(newer.hasMoreTokens())	
							newer.nextToken();
						if(newer.hasMoreTokens())
							popularity = Float.parseFloat(newer.nextToken());
						Float out=average*ratio*popularity;
						output4.write(out.toString());
						output4.newLine();
						in2.close();
					}
				}
				in3.close();
			}
			in4.close();
			output4.close();
			BufferedReader in5 = new BufferedReader(new FileReader(path+"/output2/part-00000"));
			String inputLine5=null;
			File file5 = new File(path+"/final_Producer.txt");
			BufferedWriter output5 = new BufferedWriter(new FileWriter(file5));
			while((inputLine5 = in5.readLine())!=null)
			{
				StringTokenizer newer = new StringTokenizer(inputLine5);
				String movie_name = newer.nextToken();
				StringTokenizer newer1 = new StringTokenizer(movie_name,"+");
				String movie = null;
				while(newer1.hasMoreTokens())
				{
					if(movie==null)
					{
						movie = newer1.nextToken();
					}
					else
					{
						movie = movie + " " +newer1.nextToken();
					}
				}
				BufferedReader in3 = new BufferedReader(new FileReader(path+"/ProducerPermuteMovies.txt"));
				String inputLine2=null;
				in3.readLine();
				while((inputLine2 = in3.readLine())!=null)
				{
					StringTokenizer newer2 = new StringTokenizer(inputLine2,"(");
					String move_act = newer2.nextToken();
					String movie1=newer2.nextToken();
					if(movie1.substring(0, movie1.length()-1).equals(movie))
					{
						StringTokenizer newer3= new StringTokenizer(move_act,",");
						String Act1=newer3.nextToken();
						String Act2=newer3.nextToken();
						Act2 = Act2.substring(0, Act2.length()-2);
						Act1=Act1.replace('_', '+');
						Act2 = Act2.replace('_', '+');
						BufferedReader in2 = new BufferedReader(new FileReader(path+"/output8/part-00000"));
						String inputLine3 = null;
						Float Act1R=0f,Act2R=0f;
						while((inputLine3=in2.readLine())!=null)
						{
							StringTokenizer Actors = new StringTokenizer(inputLine3);
							String Act3 = Actors.nextToken();
							String Rate = Actors.nextToken();
							if(Act3.equals(Act1))
							{
								Act1R = Float.parseFloat(Rate);
							}
							if(Act3.equals(Act2))
							{
								Act2R = Float.parseFloat(Rate);
							}
							
							if(!Act1.contains(Act2))
							{
								Act1=Act1+Act2;
								Act1R=Act1R+Act2R;
							}
						}
						StringTokenizer count = new StringTokenizer(Act1);
						Float average = Act1R/count.countTokens();
						Float ratio = 1f;
						if(newer.hasMoreTokens())
							ratio = Float.parseFloat(newer.nextToken());
						if(newer.hasMoreTokens())
							newer.nextToken();
						Float popularity = 1f;
						if(newer.hasMoreTokens())	
							popularity = Float.parseFloat(newer.nextToken());
						Float out=average*ratio*popularity;
						output5.write(out.toString());
						output5.newLine();
						in2.close();
					}
				}
				in3.close();
			}
			in5.close();
			output5.close();
			BufferedReader in6 = new BufferedReader(new FileReader(path+"/output3/part-00000"));
			String inputLine6=null;
			File file6 = new File(path+"/final_DirectorActor.txt");
			BufferedWriter output6 = new BufferedWriter(new FileWriter(file6));
			while((inputLine6 = in6.readLine())!=null)
			{
				StringTokenizer newer = new StringTokenizer(inputLine6);
				String movie_name = newer.nextToken();
				StringTokenizer newer1 = new StringTokenizer(movie_name,"+");
				String movie = null;
				while(newer1.hasMoreTokens())
				{
					if(movie==null)
					{
						movie = newer1.nextToken();
					}
					else
					{
						movie = movie + " " +newer1.nextToken();
					}
				}
				BufferedReader in3 = new BufferedReader(new FileReader(path+"/ActorDirectorPermuteMovies.txt"));
				String inputLine2=null;
				in3.readLine();
				while((inputLine2 = in3.readLine())!=null)
				{
					StringTokenizer newer2 = new StringTokenizer(inputLine2,"(");
					String move_act = newer2.nextToken();
					String movie1=newer2.nextToken();
					if(movie1.substring(0, movie1.length()-1).equals(movie))
					{
						StringTokenizer newer3= new StringTokenizer(move_act,",");
						String Act1=newer3.nextToken();
						String Act2=newer3.nextToken();
						Act2 = Act2.substring(0, Act2.length()-2);
						Act1=Act1.replace('_', '+');
						Act2 = Act2.replace('_', '+');
						BufferedReader in2 = new BufferedReader(new FileReader(path+"/output9/part-00000"));
						String inputLine3 = null;
						Float Act1R=0f,Act2R=0f;
						while((inputLine3=in2.readLine())!=null)
						{
							StringTokenizer Actors = new StringTokenizer(inputLine3);
							String Act3 = Actors.nextToken();
							String Rate = Actors.nextToken();
							if(Act3.equals(Act1))
							{
								Act1R = Float.parseFloat(Rate);
							}
							if(Act3.equals(Act2))
							{
								Act2R = Float.parseFloat(Rate);
							}
							
							if(!Act1.contains(Act2))
							{
								Act1=Act1+Act2;
								Act1R=Act1R+Act2R;
							}
						}
						StringTokenizer count = new StringTokenizer(Act1);
						Float average = Act1R/count.countTokens();
						Float ratio = 1f;
						Float popularity = 1f;
						if(newer.hasMoreTokens())
							ratio = Float.parseFloat(newer.nextToken());
						if(newer.hasMoreTokens())
							newer.nextToken();
						if(newer.hasMoreTokens())
							popularity = Float.parseFloat(newer.nextToken());
						Float out=average*ratio*popularity;
						output6.write(out.toString());
						output6.newLine();
						in2.close();
					}
				}
				in3.close();
			}
			in6.close();
			output6.close();
			BufferedReader in7 = new BufferedReader(new FileReader(path+"/output4/part-00000"));
			String inputLine7=null;
			File file7 = new File(path+"/final_ProducerActor.txt");
			BufferedWriter output7 = new BufferedWriter(new FileWriter(file7));
			while((inputLine7 = in7.readLine())!=null)
			{
				StringTokenizer newer = new StringTokenizer(inputLine7);
				String movie_name = newer.nextToken();
				StringTokenizer newer1 = new StringTokenizer(movie_name,"+");
				String movie = null;
				while(newer1.hasMoreTokens())
				{
					if(movie==null)
					{
						movie = newer1.nextToken();
					}
					else
					{
						movie = movie + " " +newer1.nextToken();
					}
				}
				BufferedReader in3 = new BufferedReader(new FileReader(path+"/ActorProducerPermuteMovies.txt"));
				String inputLine2=null;
				in3.readLine();
				while((inputLine2 = in3.readLine())!=null)
				{
					StringTokenizer newer2 = new StringTokenizer(inputLine2,"(");
					String move_act = newer2.nextToken();
					String movie1=newer2.nextToken();
					if(movie1.substring(0, movie1.length()-1).equals(movie))
					{
						StringTokenizer newer3= new StringTokenizer(move_act,",");
						String Act1=newer3.nextToken();
						String Act2=newer3.nextToken();
						Act2 = Act2.substring(0, Act2.length()-2);
						Act1=Act1.replace('_', '+');
						Act2 = Act2.replace('_', '+');
						BufferedReader in2 = new BufferedReader(new FileReader(path+"/output10/part-00000"));
						String inputLine3 = null;
						Float Act1R=0f,Act2R=0f;
						while((inputLine3=in2.readLine())!=null)
						{
							StringTokenizer Actors = new StringTokenizer(inputLine3);
							String Act3 = Actors.nextToken();
							String Rate = Actors.nextToken();
							if(Act3.equals(Act1))
							{
								Act1R = Float.parseFloat(Rate);
							}
							if(Act3.equals(Act2))
							{
								Act2R = Float.parseFloat(Rate);
							}
							
							if(!Act1.contains(Act2))
							{
								Act1=Act1+Act2;
								Act1R=Act1R+Act2R;
							}
						}
						StringTokenizer count = new StringTokenizer(Act1);
						Float average = Act1R/count.countTokens();
						Float ratio = 1f;
						Float popularity = 1f;
						if(newer.hasMoreTokens())
							ratio = Float.parseFloat(newer.nextToken());
						if(newer.hasMoreTokens())
							newer.nextToken();
						if(newer.hasMoreTokens())
							popularity = Float.parseFloat(newer.nextToken());
						Float out=average*ratio*popularity;
						output7.write(out.toString());
						output7.newLine();
						in2.close();
					}
				}
				in3.close();
			}
			in7.close();
			output7.close();
			BufferedReader in8 = new BufferedReader(new FileReader(path+"/output5/part-00000"));
			String inputLine8=null;
			File file8 = new File(path+"/final_ProducerDirector.txt");
			BufferedWriter output8 = new BufferedWriter(new FileWriter(file8));
			while((inputLine8 = in8.readLine())!=null)
			{
				StringTokenizer newer = new StringTokenizer(inputLine8);
				String movie_name = newer.nextToken();
				StringTokenizer newer1 = new StringTokenizer(movie_name,"+");
				String movie = null;
				while(newer1.hasMoreTokens())
				{
					if(movie==null)
					{
						movie = newer1.nextToken();
					}
					else
					{
						movie = movie + " " +newer1.nextToken();
					}
				}
				BufferedReader in3 = new BufferedReader(new FileReader(path+"/DirectorProducerPermuteMovies.txt"));
				String inputLine2=null;
				in3.readLine();
				while((inputLine2 = in3.readLine())!=null)
				{
					StringTokenizer newer2 = new StringTokenizer(inputLine2,"(");
					String move_act = newer2.nextToken();
					String movie1=newer2.nextToken();
					if(movie1.substring(0, movie1.length()-1).equals(movie))
					{
						StringTokenizer newer3= new StringTokenizer(move_act,",");
						String Act1=newer3.nextToken();
						String Act2=newer3.nextToken();
						Act2 = Act2.substring(0, Act2.length()-2);
						Act1=Act1.replace('_', '+');
						Act2 = Act2.replace('_', '+');
						BufferedReader in2 = new BufferedReader(new FileReader(path+"/output11/part-00000"));
						String inputLine3 = null;
						Float Act1R=0f,Act2R=0f;
						while((inputLine3=in2.readLine())!=null)
						{
							StringTokenizer Actors = new StringTokenizer(inputLine3);
							String Act3 = Actors.nextToken();
							String Rate = Actors.nextToken();
							if(Act3.equals(Act1))
							{
								Act1R = Float.parseFloat(Rate);
							}
							if(Act3.equals(Act2))
							{
								Act2R = Float.parseFloat(Rate);
							}
							
							if(!Act1.contains(Act2))
							{
								Act1=Act1+Act2;
								Act1R=Act1R+Act2R;
							}
						}
						StringTokenizer count = new StringTokenizer(Act1);
						Float average = Act1R/count.countTokens();
						Float ratio = 1f, popularity = 1f;
						if(newer.hasMoreTokens())
							ratio = Float.parseFloat(newer.nextToken());
						if(newer.hasMoreTokens())
							newer.nextToken();
						if(newer.hasMoreTokens())
							popularity = Float.parseFloat(newer.nextToken());
						Float out=average*ratio*popularity;
						output8.write(out.toString());
						output8.newLine();
						in2.close();
					}
				}
				in3.close();
			}
			in8.close();
			output8.close();
			FinalComp1.cool(path);
		}
}