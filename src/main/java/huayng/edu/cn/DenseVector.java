package huayng.edu.cn;

import java.util.Arrays;
import java.util.Map;

public class DenseVector implements Vector {
    private double[] values;

    private int size;

    public DenseVector(double[] values) {
        this.size = values.length;
        this.values = values.clone();
    }

    public DenseVector() {}

    public DenseVector(int size) {
        this.size = size;
        this.values = new double[size];
    }

    @Override
    public Vector plus(double x) {
        return null;
    }

    @Override
    public Vector plus(Vector x) {
        return null;
    }

    @Override
    public double zSum() {
        double sum =0 ;
        for(int i=0;i<this.size;i++) {
            sum += this.values[i];
        }
        return sum;
    }

    @Override
    public Vector clone() {
        return new DenseVector(this.values);
    }

    @Override
    public Vector times() { return null; }

    public Vector times(Vector vector) {
        DenseVector denseVector = new DenseVector(this.size);
        for(int i=0; i<this.size;i++) {
            denseVector.set(i,get(i)*vector.get(i));
        }
        return denseVector;
    }

    @Override
    public Vector divide(double x) {
        DenseVector vector = new DenseVector(this.size);
        for(int i=0;i<this.size;i++) {
            vector.set(i,this.values[i]/x);
        }
        return vector;
    }

    @Override
    public Vector multi(double x) {
        DenseVector denseVector = new DenseVector(this.size);
        for(int i=0; i<this.size; i++) {
            denseVector.set(i,this.values[i]*x);
        }
        return denseVector;
    }

    @Override
    public Vector minus(Vector v) {
        assert (this.size == v.getSize());
        DenseVector denseVector = new DenseVector(this.size);
        for(int i=0; i<this.size; i++) {
            denseVector.set(i,this.get(i)-v.get(i));
        }
        return denseVector;
    }

    @Override
    public Vector like() {
        return null;
    }

    @Override
    public int getSize() {
        return this.size;
    }

    public void setSize(int size) { this.size = size; }

    @Override
    public Map<Integer, Double> get() {
        return null;
    }

    @Override
    public double get(int index) {return this.values[index]; }

    public double getValue(int index) {
        return this.values[index];
    }

    @Override
    public void set(int index, Double value) {
        this.values[index] = value;
    }

    @Override
    public void clean() {
        this.size = 0;
        this.values = new double[0];
    }

    public int maxValueIndex() {
        int result = -1;
        double max = Double.NEGATIVE_INFINITY;
        for(int i=0; i < this.size; i++) {
            if(this.values[i] > max) {
                result = i;
                max = this.values[i];
            }
        }
        return result;
    }

    public Vector assign(double value) {
        Arrays.fill(this.values,value);
        return this;
    }
}
