package jp.ac.nii.exercise3;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;

import jp.ac.nii.mapreduceframework.FileInputFormat;
import jp.ac.nii.mapreduceframework.FileOutputFormat;
import jp.ac.nii.mapreduceframework.Job;
import jp.ac.nii.mapreduceframework.util.Util;

/**
 * TODO: このファイルは未完成です！
 */
public class Excercise3Main {
	public static void main(String[] args)
			throws FileNotFoundException, InstantiationException, IllegalAccessException, UnsupportedEncodingException {
		
		// TODO: 課題3： bocchan.txt を対象として、日本語版のワードカウントを作ってください。
		// また、jp.ac.nii.exercise3.Exercise3Test のテストが通ることを確認して下さい。

		// 以下は日本語の文章から単語を抽出する形態素解析のサンプルコードです。
		//for (String word : Tokenizer.tokenize("寿司を食べたい。")) {
		//	System.out.println(word);
		//}
				
		Job<Long, String, String, Integer, String, Integer> job = Job.getInstance();
		
		job.setInputFormatClass(FileInputFormat.class);
		FileInputFormat.addInputPath(job, Paths.get("bocchan.txt"));
		job.setOutputFormatClass(FileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, Paths.get("exercise3"));
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(10);
		
		job.waitForCompletion();
		
		// exercise3/outputディレクトリの中身をexercise3.tsvにマージします。
		Util.merge(Paths.get("exercise3", "output").toFile(), Paths.get("exercise3.tsv").toFile());
	}
}