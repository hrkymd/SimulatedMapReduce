package jp.ac.nii.exercise4;

import jp.ac.nii.mapreduceframework.Context;
import jp.ac.nii.mapreduceframework.Reducer;

public class StandardDeviationCalculationReducer extends Reducer<String, Integer, String, Double> {
	@Override
	protected void reduce(String key, Iterable<Integer> values, Context context){
		// TODO: 分散を計算しよう
		// ヒント1: context.getConfiguration().get() メソッドを使おう！
		// ヒント2: Excercise4Main.createConfiguration()メソッドをよく読もう！
		Double mean = Double.parseDouble(context.getConfiguration().get(key));
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
