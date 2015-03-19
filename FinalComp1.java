/* This file implements Pig Script to average out all the values in respective files computed before
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.scripting.Pig;

public class FinalComp1 extends ActorRatingBudgFIle{
	public static void cool(String path) throws IOException
	{
		Properties props = new Properties();
		PigContext context = new PigContext();
		context.setExecType(null);
		HExecutionEngine a = new HExecutionEngine(context);
		props.setProperty("fs.default.name", "hdfs://<namenode-hostname>:<port>");
		props.setProperty("mapred.job.tracker", "<jobtracker-hostname>:<port>");
		PigServer pigServer = new PigServer(a.LOCAL);
		pigServer.registerQuery("A = load '" + path + "/final_Actor.txt" + "' AS (value:float);");
		pigServer.registerQuery("B = GROUP A ALL;");
		pigServer.registerQuery("Average = FOREACH B GENERATE AVG(A);");
		pigServer.registerQuery("STORE Average into 'output.out';");
		pigServer.registerQuery("A1 = load '" + path + "/final_Director.txt" + "' AS (value:float);");
		pigServer.registerQuery("B1 = GROUP A1 ALL;");
		pigServer.registerQuery("Average1 = FOREACH B1 GENERATE AVG(A1);");
		pigServer.registerQuery("STORE Average1 into 'output1.out';");
		pigServer.registerQuery("A2 = load '" + path + "/final_Producer.txt" + "' AS (value:float);");
		pigServer.registerQuery("B2 = GROUP A2 ALL;");
		pigServer.registerQuery("Average2 = FOREACH B2 GENERATE AVG(A2);");
		pigServer.registerQuery("STORE Average2 into 'output2.out';");
		pigServer.registerQuery("A3 = load '" + path + "/final_DirectorActor.txt" + "' AS (value:float);");
		pigServer.registerQuery("B3 = GROUP A3 ALL;");
		pigServer.registerQuery("Average3 = FOREACH B3 GENERATE AVG(A3);");
		pigServer.registerQuery("STORE Average3 into 'output3.out';");
		pigServer.registerQuery("A4 = load '" + path + "/final_ProducerActor.txt" + "' AS (value:float);");
		pigServer.registerQuery("B4 = GROUP A4 ALL;");
		pigServer.registerQuery("Average4 = FOREACH B4 GENERATE AVG(A4);");
		pigServer.registerQuery("STORE Average4 into 'output4.out';");
	    pigServer.registerQuery("A5 = load '" + path + "/final_ProducerDirector.txt" + "' AS (value:float);");
		pigServer.registerQuery("B5 = GROUP A5 ALL;");
		pigServer.registerQuery("Average5 = FOREACH B5 GENERATE AVG(A5);");
		pigServer.registerQuery("STORE Average5 into 'output5.out';");
		StringTokenizer newtoken = new StringTokenizer(path,"/");
		String newpath = null;
		while(newtoken.hasMoreTokens())
		{
			if(newpath==null)
			{
				newpath=newtoken.nextToken();
			}
			else
			{
				if(newtoken.hasMoreTokens())
				{
					newpath=newpath+newtoken.nextToken();
				}
			}
		}
		if(newpath!=null)
		{
			double result = FinalComp2.comp();
			BufferedReader in = new BufferedReader(new FileReader(path+"/Budget_Popularity.txt"));
			String pop = in.readLine();
			StringTokenizer temp = new StringTokenizer(pop,":");
			temp.nextToken();
			String pop1 = temp.nextToken();
			Long budget = Long.parseLong(pop1);
			BufferedWriter output = new BufferedWriter(new FileWriter(path + "/result.txt"));
			budget = (long)(result*budget);
			output.write("Estimated Revenue: $" + budget);
			output.close();
			in.close();
		}
	}
}
