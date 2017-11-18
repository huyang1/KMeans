package huayng.edu.cn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterWritable implements Writable{
    private Cluster clusters;

    public ClusterWritable(Cluster first) { this.clusters = first; }

    public ClusterWritable() {}

    public void setValue(Cluster value) {
        this.clusters = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
