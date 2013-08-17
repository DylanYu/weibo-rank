package filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import com.csvreader.CsvReader;

public class ProcessDataSet {

	public static void process() throws IOException {
//		FileInputStream inStream = new FileInputStream("Data - KNN Data Set/rawTrainingSet");
//		FileInputStream inStream = new FileInputStream("Data - KNN Data Set/rawTestSet");
		FileInputStream inStream = new FileInputStream("weibo_data_merged/part-r-00000");
		CsvReader reader = new CsvReader(inStream, Charset.forName("utf-8"));
		
//		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/trainingSet");
//		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/testSet");
		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/weibo_data_for_classify");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream, "utf-8"));

		while (reader.readRecord()) {
			String userID = reader.get(0);
			String screenName = reader.get(1);
			String sex = reader.get(2);
			String vipDsc = reader.get(3);
			String selfDsc = reader.get(4);
			String district = reader.get(5);
			String userName = reader.get(6);
			String followingN = reader.get(7);
			String followerN = reader.get(8);
			String weiboN = reader.get(9);
			String job = reader.get(10);
			String edu = reader.get(11);
			String portrait = reader.get(12);
			String isVIP = reader.get(13);
			String tags = reader.get(14);
			String birth = reader.get(15);
			String qq = reader.get(16);
			String msn = reader.get(17);
			String email = reader.get(18);
			String createdTime = reader.get(19);
			String watchList = reader.get(20);
			String isMember = reader.get(21);
			String isTalent = reader.get(22);
			String level = reader.get(23);
			String clazz = reader.get(24).equals("") ? "0" : reader.get(24);
			
			String _userID = userID;
			int _screenName = screenName.length();
			int _sex = (sex.equals("男")) ? 0 : 1;
			int _vipDsc = vipDsc.length();
			int _selfDsc = selfDsc.length();
			int _district = district.length();
			int _userName = userName.length();
			String _followingN = followingN;
			String _followerN = followerN;
			String _weiboN = weiboN;
			int _jobLen = job.length();
			int _edu = edu.length();
			int _portrait = portrait.equals("") ? 0 : 1;
			String _isVIP = isVIP;
			int _tags = tags.length();
			int _birth = birth.equals("") ? 0 : 1;
			int _qq = qq.equals("") ? 0 : 1;
			int _msn = msn.equals("") ? 0 : 1;
			int _email = email.equals("") ? 0 : 1;
			int _createdTime = createdTime.equals("") ? 0 : 1;
			int _watchList = watchList.split(",").length;
			String _isMember = isMember;
			String _isTalent = isTalent;
			String _level = level;
			String _clazz = clazz;
			bw.write(_userID + "," + 
					_screenName + "," + 
					_sex + "," + 
					_vipDsc + "," + 
					_selfDsc + "," + 
					_district + "," + 
					_userName + "," + 
					_followingN + "," + 
					_followerN + "," + 
					_weiboN + "," + 
					_jobLen + "," + 
					_edu + "," + 
					_portrait + "," + 
					_isVIP + "," + 
					_tags + "," + 
					_birth + "," + 
					_qq + "," + 
					_msn + "," + 
					_email + "," + 
					_createdTime + "," + 
					_watchList + "," + 
					_isMember + "," + 
					_isTalent + "," + 
					_level + "," +
					_clazz + 
					"\n");
		}
		inStream.close();
		bw.close();
	}
	
	public static void main(String[] args) throws IOException {
		ProcessDataSet.process();
	}
}
