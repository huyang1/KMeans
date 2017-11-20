package huyang.edu.cn;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public final class VectorWritable extends Configured implements Writable {
  private Vector vector;

  public VectorWritable(Vector vector) {
    this.vector = vector;
  }

  public VectorWritable() {
    vector = new sampleVector();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(vector.getSize());
    for(Map.Entry<Integer, Double> entry : vector.get().entrySet()) {
      dataOutput.writeDouble(entry.getValue());
    }

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.vector.clean();
    int size = dataInput.readInt();
    for(int i=0; i<size ;i++) {
      vector.set(i,dataInput.readDouble());
    }
  }

  public Vector get() {
    return vector;
  }

  public static void writeVector(DataOutput out, Vector vector) throws IOException{
    out.writeInt(vector.getSize());
    for(Map.Entry<Integer, Double> entry : vector.get().entrySet()) {
      out.writeDouble(entry.getValue());
    }
  }

  public static Vector readVector(DataInput in) throws IOException {
    sampleVector vector = new sampleVector();
    int size = in.readInt();
    for(int i=0; i<size ;i++) {
      vector.set(i,in.readDouble());
    }
    return vector;
  }

  public VectorWritable ToSampleVector() throws Exception{
    if(this.vector instanceof DenseVector) {
      return new VectorWritable(((DenseVector)this.vector).ToSampleVector());
    }
    return this;
  }

}
