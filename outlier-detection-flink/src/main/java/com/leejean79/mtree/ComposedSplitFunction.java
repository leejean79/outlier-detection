package com.leejean79.mtree;

import com.leejean79.bean.Data;
import com.leejean79.mtree.utils.Pair;

import java.util.Set;

/**
 * A {@linkplain SplitFunction split function} that is defined by composing
 * a {@linkplain PromotionFunction promotion function} and a
 * {@linkplain PartitionFunction partition function}.
 *
 * @param <Data>> The type of the data objects.
 */
public class ComposedSplitFunction<Data> implements SplitFunction<Data> {

	private PromotionFunction<Data> promotionFunction;
	private PartitionFunction<Data> partitionFunction;

	/**
	 * The constructor of a {@link SplitFunction} composed by a
	 * {@link PromotionFunction} and a {@link PartitionFunction}.
	 */
	public ComposedSplitFunction(
			PromotionFunction<Data> promotionFunction,
			PartitionFunction<Data> partitionFunction
		)
	{
		this.promotionFunction = promotionFunction;
		this.partitionFunction = partitionFunction;
	}

	
	@Override
	public SplitResult<Data> process(Set<Data> dataSet, DistanceFunction<? super Data> distanceFunction) {
		Pair<Data> promoted = promotionFunction.process(dataSet, distanceFunction);
		Pair<Set<Data>> partitions = partitionFunction.process(promoted, dataSet, distanceFunction);
		return new SplitResult<Data>(promoted, partitions);
	}

}
