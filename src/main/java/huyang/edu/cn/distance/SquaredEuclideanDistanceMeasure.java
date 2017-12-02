package huyang.edu.cn.distance;

import huyang.edu.cn.Vector;

/**
 * Like {@link EuclideanDistanceMeasure} but it does not take the square root.
 * <p/>
 * Thus, it is not actually the Euclidean Distance, but it is saves on computation when you only need the
 * distance for comparison and don't care about the actual value as a distance.
 */
public class SquaredEuclideanDistanceMeasure implements DistanceMeasure {


  @Override
  public double distance(Vector v1, Vector v2) {
    return (Double) v2.minus(v1).times().zSum();
  }

}

