package jp.ac.nii.exercise5;

import jp.ac.nii.mapreduceframework.Context;
import jp.ac.nii.mapreduceframework.Reducer;

public class AverageCalculationReducer extends Reducer<String, Integer, String, Double> {
	@Override
	protected void reduce(String key, Iterable<Integer> values, Context context) {
		// TODO: 平均値を計算しよう（Excercise4と同じ）
		double sum = 0.0;
		int count = 0;
		for(Integer value: values){
			sum += value;
			count += 1;
		}
		Double ave;
		if(count != 0){
			ave = (Double)(sum / count);
		} else{
			ave = 0.0;
		}
		context.write(key, ave);
	}
}
