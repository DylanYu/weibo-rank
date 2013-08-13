package weiborank;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import com.csvreader.CsvReader;

public class HandleCsvData {
	
	private static final int COLUMN_COUNT = 24;
	
	private String mFileName;
	private CsvReader mReader;
	
	public HandleCsvData(String filename) throws FileNotFoundException {
		mFileName = filename;
		mReader = new CsvReader(new FileInputStream(mFileName), Charset.forName("utf-8"));
	}
	
	public void handle(String outFileName) throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFileName), "utf-8"));
		
		mReader.readHeaders();
		mReader.getHeaderCount();
		int lineCount = 0;
		int hasWatchListCount = 0;
		while (mReader.readRecord()) {
//			if (lineCount > 10) break;
			String watchList = mReader.get(COLUMN_COUNT - 4) ;
			if (!watchList.equals(""))
				hasWatchListCount++;
//			String text = "";
//			for (int i = 0; i < COLUMN_COUNT; i++) {
//				text += mReader.get(i) + "| ";
//			}
//			System.out.println(text);
//			bw.write(text + "\n");
			lineCount++;
		}
		System.out.println(hasWatchListCount + "/" + lineCount);
		bw.close();
	}
	
	
	public static void main(String[] args) {
		try {
			HandleCsvData handler = new HandleCsvData("/home/helo/weibo_test.csv");
//			HandleCsvData handler = new HandleCsvData("/home/helo/weibo_data/ice563102472-25551-2013-03-20-17-27-48-result.dat.csv");
			handler.handle("/home/helo/weibo_test_processed");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
