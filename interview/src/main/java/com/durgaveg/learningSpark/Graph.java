package com.durgaveg.learningSpark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;


/**
 * Transitive closure on a graph, implemented in Java.
 * Usage: JavaTC [partitions]
 */
public final class Graph {

  private static final int numEdges = 20;
  private static final int numVertices = 100;
  private static final Random rand = new Random(42);
  JavaSparkContext jsc ;

  static List<Tuple2<Integer, Integer>> generateGraph() {
    Set<Tuple2<Integer, Integer>> edges = new HashSet<>(numEdges);
    while (edges.size() < numEdges) {
      int from = rand.nextInt(numVertices);
      int to = rand.nextInt(numVertices);
      Tuple2<Integer, Integer> e = new Tuple2<>(from, to);
      if (from != to) {
        edges.add(e);
      }
    }
    return new ArrayList<>(edges);
  }

  static class ProjectFn implements PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>,
      Integer, Integer> {
    static final ProjectFn INSTANCE = new ProjectFn();
//maptoPair<vertex1,tuple<vertex2,vetrex3>>
    @Override
    //<b<a,f>>
    // y->z, x->y => y->z, y->x == y-><z,x> ==> x,z 
    // y,z x,y ==> x,z == result
    //union <y,z><x,y><x,z> A+B = <y,z><x,y> <x,z>
    public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> triple) {
      return new Tuple2<>(triple._2()._2(), triple._2()._1());
      //vertex3,vertex2
    }
  }

  public void run(int... args) {
	  Integer slices = (args.length > 0) ? args[0]: 2;
	  // slices mean partition => worker nodes.
	    JavaPairRDD<Integer, Integer> tc = jsc.parallelizePairs(generateGraph(), slices).cache();
	    System.out.println("Initial TC values are "+tc.collect());
	    // Linear transitive closure: each round grows paths by one edge,
	    // by joining the graph's edges with the already-discovered paths.
	    // e.g. join the path (y, z) from the TC with the edge (x, y) from
	    // the graph to obtain the path (x, z).

	    // Because join() joins on keys, the edges are stored in reversed order.
	    JavaPairRDD<Integer, Integer> edges = tc.mapToPair(e -> new Tuple2<>(e._2(), e._1()));
	    
	    System.out.println("Edges values are "+edges.collect());
	    long oldCount;
	    long nextCount = tc.count();
	    do {
	      oldCount = nextCount;
	      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
	      // then project the result to obtain the new (x, z) paths.
	      tc = tc.union(tc.join(edges).mapToPair(ProjectFn.INSTANCE)).distinct().cache();
	      nextCount = tc.count();
	    } while (nextCount != oldCount);
	    System.out.println("TC values are "+tc.distinct().collect());
	    System.out.println("TC has " + tc.count() + " edges.");
	    //spark.stop();
	   
  }
  
}