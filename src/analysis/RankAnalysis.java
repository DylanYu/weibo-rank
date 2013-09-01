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
	private static final double FANS_NUMBER = 5634827.0;
	private static final double RANK_NUMBER = 4975687.0;
	
	public static void analysis(String fansCountFile, String WeiboRankFile, String outputFile) throws IOException {
		HashMap<Long, Long> fansMap = new HashMap<Long, Long>(6000000);
//		HashMap<Integer, Double> rankMap = new HashMap<Integer, Double>(4500000);
		FileInputStream inFans = new FileInputStream(fansCountFile);
		FileInputStream inRank = new FileInputStream(WeiboRankFile);
		BufferedReader brFans = new BufferedReader(new InputStreamReader(inFans));
		BufferedReader brRank = new BufferedReader(new InputStreamReader(inRank));
		
		String line = "";
		long fansCount = 1;
		while ((line = brFans.readLine()) != null) {
			System.out.println("fans:" + fansCount);
			String[] info = line.split("\t");
			long userID = Long.parseLong(info[0]);
			//long fansCoff = (long) (fansRank * INFU_NUMBER);
			fansMap.put(userID, fansCount);
			fansCount++;
		}
		brFans.close();
		
		FileOutputStream out = new FileOutputStream(outputFile);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		line = "";
		long weiboRank = 1;
		long maxDiff = -1;
		long maxDiffLine = -1;
		while ((line = brRank.readLine()) != null) {
			System.out.println("rank:" + weiboRank);
			String[] info = line.split("\t");
			long userID = Long.parseLong(info[1]);
			long fCount = fansMap.get(userID);
			long diff = (fCount - weiboRank);
			if (Math.abs(diff) > maxDiff) {
					maxDiff = diff;
					maxDiffLine = weiboRank;
			}
			bw.write(userID + "\t" + weiboRank + "\t"  + fCount + "\t" + diff + "\n");
			weiboRank++;
		}
		System.out.println("Max Diff is: " + maxDiff + " at " + maxDiffLine);
		brRank.close();
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
		RankAnalysis.analysis(args[0], args[1], args[2]);
//		RankAnalysis.analysis2();
	}
}
