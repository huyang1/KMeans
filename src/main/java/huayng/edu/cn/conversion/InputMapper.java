package huayng.edu.cn.conversion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

public class InputMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {

  private static final Pattern SPACE = Pattern.compile(" ");

  private Constructor<?> constructor;

  @Override
  protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

    String[] numbers = SPACE.split(values.toString());//未考虑出现缺省值的情况
    // sometimes there are multiple separator spaces
    Collection<Double> doubles = new ArrayList<>();
    for (String value : numbers) {
      if (!value.isEmpty()) {
        doubles.add(Double.valueOf(value));
      }
    }
    // ignore empty lines in data file
    if (!doubles.isEmpty()) {
      try {
        Vector result = (Vector) constructor.newInstance(doubles.size());
        int index = 0;
        for (Double d : doubles) {
          result.set(index++, d);
        }
        VectorWritable vectorWritable = new VectorWritable(result);
        context.write(new Text(String.valueOf(index)), vectorWritable);

      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    String vectorImplClassName = conf.get("vector.implementation.class.name");
    try {
      Class<? extends Vector> outputClass = conf.getClassByName(vectorImplClassName).asSubclass(Vector.class);
      constructor = outputClass.getConstructor(int.class);
    } catch (NoSuchMethodException | ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

}
