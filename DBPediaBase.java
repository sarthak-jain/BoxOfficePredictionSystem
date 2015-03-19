/* The DBPedia API will be used to gather the list of movies that a certain actor from the actors list has previously worked in, a certain producer has made, a certain director has worked on, 
 * and a combination of actor-director, actor-producer and director-producer and the list of previous movies for them.
 * This file provides the base files for the DBPedia API configuration file.
 * It reads the files created by BoxOfficePrediction.java program and creates combination of Actor-Actor, Director-Director, Producer-Producer, Actor-Director, Actor-Producer, and Director-Producer.
 * It then writes these combinations into six different files that will serve as the input to the DBPedia API Configuration file.
 * The format of these file is kept in the form "(Actor Name) (Actor Name)", "(Director Name) (Director Name)" and so on.
 */

import java.io.*;

public class DBPediaBase extends BoxOfficePrediction {
		public static void PermuteActors(String path, String move,String RelDate) throws 
Exception
		{
			String Actors[]=new String[100];		
			int i=0;
			BufferedReader in = new BufferedReader(new FileReader(path+"/Actors.txt"));		//Same path to which BoxOfficePrediction.java stored the Actors.txt file
			String line;
			while((line=in.readLine())!= null)
			{
				Actors[i]=line;
				i++;
			}
			in.close();
			String file = path+"/ActorPermute.txt";											//Output file for Combination Actor-Actor
			File file2 = new File(file);
			BufferedWriter output = new BufferedWriter(new FileWriter(file2));
			for(i=0;i<100;i++)																//Since maximum 100 actors were stored by BoxOfficePrediction.java
			{
				if(Actors[i]!=null)
				{
					for(int j=0;j<100;j++)
					{
						if(Actors[j]!=null)
						{
							output.write("(" + Actors[i] + ")" + "(" + Actors[j] + ")");			//Also stores the (ActorName1) (ActorName1) type of record to facilitate single Actor name search
							output.newLine();
						}
					}
				}
			}
			output.close();
			BufferedReader in1 = new BufferedReader(new FileReader(path+"/Directors.txt"));				//Path of Directors.txt
			String line1;
			String Directors[] = new String[20];
			i=0;
			while((line1=in1.readLine())!=null)
			{
				Directors[i] = line1;
				i++;
			}
			in1.close();
			String file3 = path+"/DirectorPermute.txt";											// Output file for Director-Director Combinations
			File file4 = new File(file3);
			BufferedWriter output1 = new BufferedWriter(new FileWriter(file4));
			for(i=0;i<20;i++)
			{
				if(Directors[i]!=null)
				{
					for(int j=0;j<20;j++)
					{
						if(Directors[j]!=null)													//(DirectorName1) (DirectorName1) combination allowed to facilitate single director search
						{
							output1.write("(" + Directors[i] + ")" + "(" + Directors[j] + ")");
							output1.newLine();
						}
					}
				}
			}
			output1.close();
			BufferedReader in2 = new BufferedReader(new FileReader(path+"/Producers.txt"));					//Path of Producers.txt
			String line2;
			String Producers[] = new String[20];
			i=0;
			while((line2=in2.readLine())!=null)
			{
				Producers[i] = line2;
				i++;
			}
			in2.close();
			String file5 = path+"/ProducerPermute.txt";											//Output file for Producer-Producer Combinations
			File file6 = new File(file5);
			BufferedWriter output2 = new BufferedWriter(new FileWriter(file6));
			for(i=0;i<20;i++)
			{
				if(Producers[i]!=null)
				{
					for(int j=0;j<20;j++)
					{
						if(Producers[j]!=null)
						{
							output2.write("(" + Producers[i] + ")" + "(" + Producers[j] + ")");			//(ProducerName1) (ProducerName1) form allowed to facilitate single producer search
							output2.newLine();
						}
					}
				}
			}
			output2.close();
			String file7 = path + "/ActorDirectorPermute.txt";										//Output file for Actor-Director Combinations
			File file8 = new File(file7);
			BufferedWriter output3 = new BufferedWriter(new FileWriter(file8));
			for(i=0;i<100;i++)
			{
				for(int j=0;j<20;j++)
					{
						if(Directors[j]!=null && Actors[i]!=null)
						{
							output3.write("(" + Actors[i] + ")" + "(" + Directors[j] + ")");				//Actor and Director with same name allowed
							output3.newLine();
						}
					}
			}
			output3.close();
			String file9 = path + "/ActorProducerPermute.txt";									//Output file for Actor-Producer Combinations
			File file10 = new File(file9);
			BufferedWriter output4 = new BufferedWriter(new FileWriter(file10));
			for(i=0;i<100;i++)
			{
				for(int j=0;j<20;j++)
				{
					if(Producers[j]!=null && Actors[i]!=null)
					{
						output4.write("(" + Actors[i] + ")" + "(" + Producers[j] + ")");					//Actor and Producer with same name allowed
						output4.newLine();
					}
				}
			}
			output4.close();
			String file11 = path + "/DirectorProducerPermute.txt";									//Output file for Director-Producer Combinations
			File file12 = new File(file11);
			BufferedWriter output5 = new BufferedWriter(new FileWriter(file12));
			for(i=0;i<20;i++)
			{
				for(int j=0;j<20;j++)
				{
					if(Producers[j]!=null && Directors[i]!=null)
					{
						output5.write("(" + Directors[i] + ")" + "(" + Producers[j] + ")");						//Directors and Producers with same name allowed
						output5.newLine();
					}
				}
			}
			output5.close();
			DBPedia.DB(path,move,RelDate);
		}
}