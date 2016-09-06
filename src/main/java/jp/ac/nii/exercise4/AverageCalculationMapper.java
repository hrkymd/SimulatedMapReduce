package jp.ac.nii.exercise4;

import jp.ac.nii.mapreduceframework.Context;
import jp.ac.nii.mapreduceframework.Mapper;

public class AverageCalculationMapper extends Mapper<Long, String, String, Integer> {
	@Override
	protected void map(Long key, String line, Context context) {
		// TODO: 科目と点数のキーバリューに変換しよう
		String[] words = line.split(",");
		if (isWord(words[0])) {
				context.write(words[0].toLowerCase(), Integer.parseInt(words[1]));
		}
	}
	
	private boolean isWord(String word) {
		for (int i = 0; i < word.length(); i++) {
			if (!Character.isAlphabetic(word.charAt(i))) {
				return false;
			}
		}
		return word.length() > 0;
	}
}
