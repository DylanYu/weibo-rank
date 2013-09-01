package analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class ExtractFile {
	public static void extract(String inFile, String outFile, int lineCount) throws IOException {
		FileInputStream inStream = new FileInputStream(inFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(inStream, "utf-8"));
		FileOutputStream outStream = new FileOutputStream(outFile);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream, "utf-8"));
		
		String line = "";
		int count = 0;
		while ((line = br.readLine()) != null && count < lineCount) {
			bw.write(line + "\n");
			count++;
		}
		br.close();
		bw.close();
	}
	
	public static void main(String[] args) throws IOException {
		ExtractFile.extract("Analysis", "data/Diff", 5000);
	}
}
