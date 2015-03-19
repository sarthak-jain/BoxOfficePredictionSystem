/*
 *  This file implements linear regression on all the values computed till now by averaging them, thus finding the most probable ratio of Revenue/Budget of the input movie, which when multiplied by the Budget of the movie gives the estimated revenue.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

public class FinalComp2 extends FinalComp1 {
	public static Float comp() throws IOException
	{
			BufferedReader avg1 = new BufferedReader(new FileReader("output.out/part-r-00000"));
			String inputLine = avg1.readLine();
			avg1.close();
			FileUtils.deleteDirectory(new File("output.out"));
			BufferedReader avg2 = new BufferedReader(new FileReader("output1.out/part-r-00000"));
			String inputLine2 = avg2.readLine();
			avg2.close();
			FileUtils.deleteDirectory(new File("output1.out"));
			BufferedReader avg3 = new BufferedReader(new FileReader("output2.out/part-r-00000"));
			String inputLine3 = avg3.readLine();
			avg3.close();
			FileUtils.deleteDirectory(new File("output2.out"));
			BufferedReader avg4 = new BufferedReader(new FileReader("output3.out/part-r-00000"));
			String inputLine4 = avg4.readLine();
			avg4.close();
			FileUtils.deleteDirectory(new File("output3.out"));
			BufferedReader avg5 = new BufferedReader(new FileReader("output4.out/part-r-00000"));
			String inputLine5 = avg5.readLine();
			avg5.close();
			FileUtils.deleteDirectory(new File("output4.out"));
			BufferedReader avg6 = new BufferedReader(new FileReader("output5.out/part-r-00000"));
			String inputLine6 = avg6.readLine();
			avg6.close();
			FileUtils.deleteDirectory(new File("output5.out"));
			int count=0;
			Float average = 0f;
			if(inputLine!=null)
			{
				count++;
				average = average + Float.parseFloat(inputLine);
			}
			if(inputLine2!=null)
			{
				count++;
				average += Float.parseFloat(inputLine2);
			}
			if(inputLine3!=null)
			{
				count++;
				average += Float.parseFloat(inputLine3);
			}
			if(inputLine4!=null)
			{
				count++;
				average += Float.parseFloat(inputLine4);
			}
			if(inputLine5!=null)
			{
				count++;
				average += Float.parseFloat(inputLine5);
			}
			if(inputLine6!=null)
			{
				count++;
				average += Float.parseFloat(inputLine6);
			}
			average = average/count;
			System.out.println(average);
			return average; 
	}
}
