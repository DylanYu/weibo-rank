package analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

/**
 * Analysis weibo rank.
 * 
 * @author Dongliang Yu
 *
 */
public class RankAnalysis {
	private static final double FANS_NUMBER = 4165343.0;
	private static final double INFU_NUMBER = 4359058.0;
	private static final int COFF = 100000;
	
	public static void analysis() throws IOException {
//		HashMap<Long, Double> fansMap = new HashMap<Long, Double>(4500000);
		HashMap<Long, Long> fansMap = new HashMap<Long, Long>(4500000);
//		HashMap<Integer, Double> rankMap = new HashMap<Integer, Double>(4500000);
		String fansFile = "C:/workspace/weiborank/Fans_rank";
		String infuFile = "C:/workspace/weiborank/Weibo_rank";
		FileInputStream inFans = new FileInputStream(fansFile);
		FileInputStream inRank = new FileInputStream(infuFile);
		BufferedReader brFans = new BufferedReader(new InputStreamReader(inFans));
		BufferedReader brInfu = new BufferedReader(new InputStreamReader(inRank));
		
		String line = "";
		int fansRank = 1;
		while ((line = brFans.readLine()) != null) {
			System.out.println("fans:" + fansRank);
			String[] info = line.split("\t");
			long userID = Long.parseLong(info[0]);
//			double fansPectg = fansRank / FANS_NUMBER;
//			fansMap.put(userID, fansPectg);
			long fansCoff = (long) (fansRank * INFU_NUMBER);
			fansMap.put(userID, fansCoff);
			fansRank++;
		}
		
		FileOutputStream out = new FileOutputStream("C:/workspace/weiborank/Analysis");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		line = "";
		int infuRank = 1;
		while ((line = brInfu.readLine()) != null) {
			System.out.println("infu:" + infuRank);
			String[] info = line.split("\t");
			long userID = Long.parseLong(info[1]);
//			double infuPectg = infuRank / INFU_NUMBER;
//			double fansPectg = fansMap.get(userID);
			
//			bw.write(userID + "\t" + infuPectg + "\t" + fansPectg + "\t" + (infuPectg - fansPectg) * COFF + "\n");
			
			long infuCoff = (long) (infuRank * FANS_NUMBER);
			long fansCoff = fansMap.get(userID);
			bw.write(userID + "\t" + infuCoff + "\t" + fansCoff + "\t" + (infuCoff - fansCoff) + "\n");
			
			infuRank++;
		}
		
		brFans.close();
		brInfu.close();
		bw.close();
	}
	
	public static void analysis2() throws IOException {
		String file = "C:/workspace/weiborank/Analysis";
		FileInputStream inStream = new FileInputStream(file);
		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
		String line = "";
		int count = 0;
		long sum = 0;
		while ((line = br.readLine()) != null && count < 1000000) {
			String[] info = line.split("\t");
			if (count >= 999900) {
				long diff = Long.parseLong(info[3]);
//				sum += Math.abs(diff);
				sum += diff;
			}
			count++;
		}
		System.out.println(sum / 100);
		br.close();
	}
	
	public static void main(String[] args) throws IOException {
//		RankAnalysis.analysis();
		RankAnalysis.analysis2();
	}
}
