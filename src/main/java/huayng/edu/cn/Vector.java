package huayng.edu.cn;


import huayng.edu.cn.function.DoubleDoubleFunction;
import huayng.edu.cn.function.DoubleFunction;

import java.util.Map;

/**
 * The basic interface including numerous convenience functions <p> NOTE: All implementing classes must have a
 * constructor that takes an int for cardinality and a no-arg constructor that can be used for marshalling the Writable
 * instance <p> NOTE: Implementations may choose to reuse the Vector.Element in the Iterable methods
 */
public interface Vector extends Cloneable {

  /**
   * Return a new vector containing the sum of each value of the recipient and the argument
   *
   * @param x a double
   * @return a new Vector
   */
  Vector plus(double x);

  /**
   * Return a new vector containing the element by element sum of the recipient and the argument
   *
   * @param x a Vector
   * @return a new Vector
   * @throws Exception if the cardinalities differ
   */
  Vector plus(Vector x);


  /**
   * Return the sum of all the elements of the receiver
   *
   * @return a double
   */
  double zSum();

  Vector clone();

  /**
   * 对向量平方
   * @return
   */
  Vector times();

  Vector divide(double x);

  Vector multi(double x);

  Vector minus(Vector v);

  Vector like();

  int getSize();

  Map<Integer, Double> get();

  double get(int index);

  void set(int index ,Double value);

  void clean();

}
