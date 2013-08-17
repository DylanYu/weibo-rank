package filter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;

import com.csvreader.CsvReader;

/**
 * Verify the produced training data.
 * 
 * @author Dongliang Yu
 *
 */
public class VerifyTraningData {
	public static void vertify() throws IOException {
		FileInputStream inStream = new FileInputStream("/home/helo/Workspace/Hadoop/Data - KNN Data Set/trainingSet");
//		FileInputStream inStream = new FileInputStream("/home/helo/Workspace/Hadoop/Data - KNN Data Set/testSet");
		CsvReader csvReader = new CsvReader(inStream, Charset.forName("utf-8"));
//		csvReader.readHeaders();
		String line = null;
		int count = 0;
		int count1 = 0;
		while (csvReader.readRecord()) {
			String userID = csvReader.get(0);
			String tag = csvReader.get(24);
			if (tag.equals("0"))
				count1++;
			count++;
		}
		System.out.println(count1 + "/" + count);
	}
	
	public static void main(String[] args) throws IOException {
		VerifyTraningData.vertify();
	}
}
