package jp.ac.nii.exercise5;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

import com.google.common.collect.Maps;

import jp.ac.nii.mapreduceframework.Context;
import jp.ac.nii.mapreduceframework.Reducer;

public class StandardDeviationCalculationReducer extends Reducer<String, Integer, String, Double> {
	private Map<String, Double> subject2Average = Maps.newHashMap();

	@Override
	protected void setup(Context context) {
		// 注意：以下は分散キャッシュ（DistributedCache）から平均値の計算結果の読み込みを行なっているが、本家Hadoopとは処理が大きく異なります！
		try {
			FileInputStream intput = new FileInputStream("exercise5_average.tsv");
			Scanner scanner = new Scanner(intput, "UTF-8");
			
			while (scanner.hasNext()) {
				// TODO: subject2Averageに科目名と平均値のキーバリューを保存しよう
				String subject = scanner.next();
				Double average = Double.parseDouble(scanner.next());
				subject2Average.put(subject , average);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(String key, Iterable<Integer> values, Context context) {
		// TODO: 分散を計算しよう
		// ヒント: subject2Average フィールドを使おう！
		
		Double mean = subject2Average.get(key);
		int count = 0;
		Double sum = 0.0;
		
		for (Integer value : values) {
			sum += (value - mean) * (value - mean);
			count += 1;
		}
		Double var = sum / (double)count;
		Double stdv = Math.sqrt(var);
		context.write(key, stdv);
		
	}
}
