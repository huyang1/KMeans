package huayng.edu.cn.distance;

import huayng.edu.cn.Vector;
import huayng.edu.cn.parameters.Parameter;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.Collections;

/**
 * Like {@link EuclideanDistanceMeasure} but it does not take the square root.
 * <p/>
 * Thus, it is not actually the Euclidean Distance, but it is saves on computation when you only need the
 * distance for comparison and don't care about the actual value as a distance.
 */
public class SquaredEuclideanDistanceMeasure implements DistanceMeasure {

  @Override
  public void configure(Configuration job) {
    // nothing to do
  }

  @Override
  public Collection<Parameter<?>> getParameters() {
    return Collections.emptyList();
  }

  @Override
  public void createParameters(String prefix, Configuration jobConf) {
    // nothing to do
  }

  @Override
  public double distance(Vector v1, Vector v2) {
    return v2.minus(v1).times().zSum();
  }

//  @Override
//  public double distance(double centroidLengthSquare, Vector centroid, Vector v) {
//    return centroidLengthSquare - 2 * v.dot(centroid) + v.getLengthSquared();
//  }
}

