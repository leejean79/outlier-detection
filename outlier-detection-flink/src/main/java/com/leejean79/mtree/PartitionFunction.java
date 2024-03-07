package com.leejean79.mtree;

import com.leejean79.mtree.utils.Pair;

import java.util.Set;

/**
 * An object with partitions a set of data into two sub-sets.
 *
 * @param <Data> The type of the data on the sets.
 */
public interface PartitionFunction<Data> {
	
	/**
	 * Executes the partitioning.
	 * 
	 * @param promoted The pair of data objects that will guide the partition
	 *        process.
	 * @param dataSet The original set of data objects to be partitioned.
	 * @param distanceFunction A {@linkplain DistanceFunction distance function}
	 *        to be used on the partitioning.
	 * @return A pair of partition sub-sets. Each sub-set must correspond to one
	 *         of the {@code promoted} data objects.
	 */
	Pair<Set<Data>> process(Pair<Data> promoted, Set<Data> dataSet, DistanceFunction<? super Data> distanceFunction);
	
}
