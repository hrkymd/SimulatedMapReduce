package jp.ac.nii.exercise6;

import java.util.Arrays;
import java.util.Comparator;

import jp.ac.nii.mapreduceframework.Context;
import jp.ac.nii.mapreduceframework.Mapper;

/**
 * 以下の式の分子（numerator）を計算するジョブのMapperです。
 *  関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 *  完成
 */
public class SpecPairAggregationMapper extends Mapper<Long, String, String, Integer> {
	@Override
	public void map(Long key, String value, Context context) {
		// TODO: 商品ペアの名前を昇順でソートした後、キーを「商品X,商品Y」という文字列、バリューを1にしてペアの頻度を計算するMapperを作ろう
		
		// 商品ペアの名前を昇順でソートすることで、例えば、「あんドーナツ,生シュークリーム」と「生シュークリーム,あんドーナツ」など、
		// 実質は同じだが順序が異なるペアを「あんドーナツ,生シュークリーム」という一つのペアに集約することができる
		
		String[] items = value.split(",");
		
		//ソート
		Arrays.sort(items, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});

		context.write(items[0] + "," + items[1], 1);	
	}
}
