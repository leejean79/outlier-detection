package com.leejean79.mtree;

import com.leejean79.bean.Data;
import com.leejean79.mtree.utils.Pair;
import com.leejean79.mtree.utils.Pair;
import com.leejean79.mtree.utils.Utils;

import java.util.List;
import java.util.Set;

/**
 * Some pre-defined implementations of {@linkplain PromotionFunction promotion
 * functions}.
 */
public final class PromotionFunctions {

    /**
     * Don't let anyone instantiate this class.
     */
	private PromotionFunctions() {}
	
	
	/**
	 * A {@linkplain PromotionFunction promotion function} object that randomly
	 * chooses ("promotes") two data objects.
	 *
	 * @param <Data> The type of the data objects.
	 */
	public static class RandomPromotion<Data> implements PromotionFunction<Data> {
		@Override
		public Pair<Data> process(Set<Data> dataSet,
                                  DistanceFunction<? super Data> distanceFunction)
		{
			List<Data> promotedList = Utils.randomSample(dataSet, 2);
			return new Pair<Data>(promotedList.get(0), promotedList.get(1));
		}
	}
	
}
