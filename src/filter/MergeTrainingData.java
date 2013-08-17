package filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Merge all labeled data and produce training and test data set.
 * 
 * @author Dongliang Yu
 *
 */
public class MergeTrainingData {
	@SuppressWarnings("unchecked")
	public static void merge() throws IOException {
		FileInputStream inStream0 = new FileInputStream("Data - KNN Data Set/0-500");
		FileInputStream inStream1 = new FileInputStream("Data - KNN Data Set/501-1000");
		FileInputStream inStream2 = new FileInputStream("Data - KNN Data Set/1001-1500");
		BufferedReader br0 = new BufferedReader(new InputStreamReader(inStream0, "utf-8"));
		BufferedReader br1 = new BufferedReader(new InputStreamReader(inStream1, "utf-8"));
		BufferedReader br2 = new BufferedReader(new InputStreamReader(inStream2, "utf-8"));
		ArrayList<String> store = new ArrayList<String>();
		ArrayList<String> shuffled = new ArrayList<String>();
		int count = 0;
		while (count < 500) {
			String line0 = br0.readLine();
//			String line1 = br1.readLine();
			String line2 = br2.readLine();
			store.add(line0);
//			store.add(line1);
			store.add(line2);
			count++;
		}
		br0.close();
//		br1.close();
		br2.close();
		shuffled = (ArrayList<String>) store.clone();
		Collections.shuffle(shuffled);
		Collections.shuffle(shuffled);
		Collections.shuffle(shuffled);
		Collections.shuffle(shuffled);
		Collections.shuffle(shuffled);
		
		FileOutputStream outStream0 = new FileOutputStream("Data - KNN Data Set/rawTrainingSet");
		BufferedWriter bw0 = new BufferedWriter(new OutputStreamWriter(outStream0, "utf-8"));
		FileOutputStream outStream1 = new FileOutputStream("Data - KNN Data Set/rawTestSet");
		BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(outStream1, "utf-8"));
		count = 0;
		while (count < 1000) {
			if (count < 700)
				bw0.write(shuffled.get(count) + "\n");
			else {
				bw1.write(shuffled.get(count) + "\n");
			}
			count++;
		}
		bw0.close();
		bw1.close();
	}
	
	public static void main(String[] args) throws IOException {
		MergeTrainingData.merge();
	}
}
