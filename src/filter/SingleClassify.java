package filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;

import weka.classifiers.Classifier;
import weka.classifiers.lazy.IBk;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

public class SingleClassify {
	public static void compute() throws Exception {
		Classifier classifier = new J48();
//		Classifier classifier = new IBk(30);
		
		DataSource trainSource = new DataSource("Data - KNN Data Set/trainingSet.arff");
		Instances 	instancesTrain = trainSource.getDataSet();
		instancesTrain.setClassIndex(instancesTrain.numAttributes() - 1);
		long t0 = new Date().getTime();
		classifier.buildClassifier(instancesTrain);
		long t1 =  new Date().getTime();
		System.out.println("Time used:" + (double)(t1 - t0) / 1000);
		
//		DataSource testSource = new DataSource("Data - KNN Data Set/testSet.arff");
//		DataSource testSource = new DataSource("Data - KNN Data Set/trainingSet.arff");
		DataSource testSource = new DataSource("Data - KNN Data Set/weibo_data_for_classify.arff");
		Instances 	instancesTest = testSource.getDataSet();
		instancesTest.setClassIndex(instancesTest.numAttributes() - 1);
		
		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/classified_result");
//		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/resultOnTestSet(J48)");
//		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/resultOnTrainingSet(J48)");
//		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/resultOnTestSet(IBk)");
//		FileOutputStream outStream = new FileOutputStream("Data - KNN Data Set/resultOnTrainingSet(IBk)");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream));
		
		double numOfInstances = instancesTest.numInstances();
		for(int i = 0; i < numOfInstances; i++) {
			System.out.println(i);
			double[] distribution;
			distribution = classifier.distributionForInstance(instancesTest.instance(i));
			//System.out.println(distribution[0] + "," + distribution[1]);
			int clazz = 0;
			if (distribution[0] < distribution[1])
				clazz = 1;
			bw.write(clazz + "\t" + distribution[0]  + "\t" + distribution[1] + "\n");
		}
		bw.close();
	}
	
	public static void evaluate() throws IOException {
		FileInputStream inStream0 = new FileInputStream("Data - KNN Data Set/classified_result");
//		FileInputStream inStream0 = new FileInputStream("Data - KNN Data Set/resultOnTrainingSet(J48)");
//		FileInputStream inStream0 = new FileInputStream("Data - KNN Data Set/resultOnTestSet(IBk)");
//		FileInputStream inStream0 = new FileInputStream("Data - KNN Data Set/resultOnTrainingSet(IBk)");
		
		FileInputStream inStream1 = new FileInputStream("Data - KNN Data Set/testSet");
//		FileInputStream inStream1 = new FileInputStream("Data - KNN Data Set/trainingSet");
		BufferedReader br0 = new BufferedReader(new InputStreamReader(inStream0));

		
		BufferedReader br1 = new BufferedReader(new InputStreamReader(inStream1));
		
		String line0 = null;
		String line1 = null;
		int correctCount = 0;
		int count = 0;
		while ((line0 = br0.readLine()) != null && (line1 = br1.readLine()) != null) {
			String computedResult = line0.split("\t")[0];
			String realResult = line1.split(",")[24];
			if (computedResult.equals(realResult))
				correctCount++;
			count++;
			System.out.println(computedResult + "," + realResult);
		}
		System.out.println(correctCount + "/" + count + ": " + (double)correctCount / count);
	}

	public static void copyResultBack() throws IOException {
		FileInputStream inStream0 = new FileInputStream("weibo_data_merged/part-r-00000");
		BufferedReader br0 = new BufferedReader(new InputStreamReader(inStream0, "utf-8"));
		FileInputStream inStream1 = new FileInputStream("Data - KNN Data Set/classified_result");
		BufferedReader br1 = new BufferedReader(new InputStreamReader(inStream1, "utf-8"));
		
		FileOutputStream outStream = new FileOutputStream("Data - Weibo_User_With_Category/weibo_user_with_category");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream, "utf-8"));
		
		String line0 = null;
		String line1 = null;
		while ((line0 = br0.readLine()) != null && (line1 = br1.readLine()) != null) {
			String noTab = line0.split("\t")[1];
			String[] strs = line1.split("\t");
			String label = strs[0];
			double coff = Double.parseDouble(strs[2]);
			// retain 3 or less digits after the decimal point
			coff = ((int)(coff * 1000)) / 1000.0;
			bw.write(noTab + "," + label + "," + coff + "\n");
		}
		
		br0.close();
		br1.close();
		bw.close();
	}
	
	public static void main(String[] args) throws Exception {
//		SingleClassify.compute();
//		SingleClassify.evaluate();
		SingleClassify.copyResultBack();
	}
}
