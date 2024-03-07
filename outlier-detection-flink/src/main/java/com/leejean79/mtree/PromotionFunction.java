package com.leejean79.mtree;

import com.leejean79.bean.Data;
import com.leejean79.mtree.utils.Pair;
import com.leejean79.mtree.utils.Pair;

import java.util.Set;

/**
 * An object that chooses a pair from a set of data objects.
 *
 * @param <Data> The type of the data objects.
 */
public interface PromotionFunction<Data> {
	
	/**
	 * Chooses (promotes) a pair of objects according to some criteria that is
	 * suitable for the application using the M-Tree.
	 * 
	 * @param dataSet The set of objects to choose a pair from.
	 * @param distanceFunction A function that can be used for choosing the
	 *        promoted objects.
	 * @return A pair of chosen objects.
	 */
	Pair<Data> process(Set<Data> dataSet, DistanceFunction<? super Data> distanceFunction);
	
}
