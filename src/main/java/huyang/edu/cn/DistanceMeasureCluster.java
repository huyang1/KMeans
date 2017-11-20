package huyang.edu.cn;

import huyang.edu.cn.distance.DistanceMeasure;
import huyang.edu.cn.parameters.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public class DistanceMeasureCluster implements Cluster,Writable{

    private boolean converged;

    private int id;

    private long numObservations;

    private long totalObservations;

    private Vector center;

    private Vector radius;

    // the observation statistics
    private  double s0;

    private  Vector s1;

    private  Vector s2;

    private DistanceMeasure measure;

    public DistanceMeasureCluster() {}

    public DistanceMeasureCluster(Vector vector, int id, DistanceMeasure measure) {
        this.numObservations = (long) 0;
        this.totalObservations = (long) 0;
        this.center = vector.clone();
        this.radius = center.like();
        this.s0 = (double) 0;
        this.s1 = center.like();
        this.s2 = center.like();
        this.id = id;
        this.measure = measure;
    }

    @Override
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public Vector getCenter() {
        return center;
    }

    protected void setCenter(Vector center) {
        this.center = center;
    }

    @Override
    public Vector getRadius() {
        return radius;
    }

    protected void setRadius(Vector radius) {
        this.radius = radius;
    }

    @Override
    public void computeParameters() {
        if (getS0() == 0) {
            return;
        }
        setNumObservations((long) getS0());
        setTotalObservations(getTotalObservations() + getNumObservations());
        setCenter(getS1().divide(getS0()));
        // compute the component stds
        if (getS0() > 1) {
            setRadius((getS2().multi(getS0()).minus(getS1().times())).square().divide(getS0()));
        }
        setS0(0d);
        setS1(center.like());
        setS2(center.like());
    }

    @Override
    public long getNumObservations() {
        return numObservations;
    }

    public void setNumObservations(long l) {
        this.numObservations = l;
    }

    @Override
    public long getTotalObservations() {
        return totalObservations;
    }

    @Override
    public void observe(Cluster x) {
        DistanceMeasureCluster cluster = (DistanceMeasureCluster) x;
        setS0(getS0()+cluster.getS0());
        setS1(getS1().plus(cluster.getS1()));
        setS2(getS2().plus(cluster.getS2()));
    }

    @Override
    public void observe(Vector x) {
        setS0(getS0() + 1);
        if (getS1() == null) {
            setS1(x.clone());
        } else {
            setS1(getS1().plus(x));
        }
        Vector x2 = x.times();
        if (getS2() == null) {
            setS2(x2);
        } else {
            setS2(getS2().plus(x2));
        }
    }

    @Override
    public void observe(Vector x, double weight) {
        if (weight == 1.0) {
            observe(x);
        } else {
            setS0(getS0() + weight);
            Vector weightedX = x.multi(weight);
            if (getS1() == null) {
                setS1(weightedX);
            } else {
                getS1().plus(weightedX);
            }
            Vector x2 = x.times().multi(weight);
            if (getS2() == null) {
                setS2(x2);
            } else {
                getS2().plus(x2);
            }
        }
    }

    public void setTotalObservations(long totalPoints) {
        this.totalObservations = totalPoints;
    }

    public Vector getS1() { return s1; }

    protected void setS0(Double s0) { this.s0 = s0; }

    public double getS0() { return s0; }

    protected void setS1(Vector s1) { this.s1 = s1; }

    public Vector getS2() { return s2; }

    protected void setS2(Vector s2) { this.s2 = s2; }


    @Override
    public boolean isConverged() {
        return converged;
    }

    protected void setConverged(boolean converged) {
        this.converged = converged;
    }

    @Override
    public double pdf(VectorWritable x) {
        return 1 / (1 + measure.distance(x.get(), getCenter()));
    }

    @Override
    public Collection<Parameter<?>> getParameters() {
        return null;
    }

    @Override
    public void createParameters(String prefix, Configuration jobConf) {
        if (getS0() == 0) {
            return;
        }
        setNumObservations((long) getS0());
        setTotalObservations(getTotalObservations() + getNumObservations());
        setCenter(getS1().divide(getS0()));
        // compute the component stds
        if (getS0() > 1) {
            setRadius((getS2().multi(getS0()).minus(getS1().times())).square().divide(getS0()));
        }
        setS0(0d);
        setS1(center.like());
        setS2(center.like());
    }

    @Override
    public void configure(Configuration config) {
        if (measure != null) {
            measure.configure(config);
        }
    }
    public DistanceMeasure getMeasure() {
        return measure;
    }

    /**
     * @param measure
     *          the measure to set
     */
    public void setMeasure(DistanceMeasure measure) {
        this.measure = measure;
    }

    public String getIdentifier() {
        return "DMC:" + getId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(measure.getClass().getName());
        out.writeBoolean(converged);
        out.writeInt(id);
        out.writeLong(getNumObservations());
        out.writeLong(getTotalObservations());
        VectorWritable.writeVector(out, getCenter());
        VectorWritable.writeVector(out, getRadius());
        out.writeDouble(s0);
        VectorWritable.writeVector(out, s1);
        VectorWritable.writeVector(out, s2);
    }

    public Vector computeCentroid() {
        return getS0() == 0 ? getCenter() : getS1().divide(getS0());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String dm = in.readUTF();
        this.measure = ClassUtils.instantiateAs(dm, DistanceMeasure.class);
        this.converged = in.readBoolean();
        this.id = in.readInt();
        this.setNumObservations(in.readLong());
        this.setTotalObservations(in.readLong());
        this.setCenter(VectorWritable.readVector(in));
        this.setRadius(VectorWritable.readVector(in));
        this.setS0(in.readDouble());
        this.setS1(VectorWritable.readVector(in));
        this.setS2(VectorWritable.readVector(in));
    }

    public boolean calculateConvergence(double convergenceDelta) {
        Vector vector = computeCentroid();
        this.converged = getMeasure().distance(vector, getCenter()) <= convergenceDelta;
        return converged;

    }
}
