package huayng.edu.cn;

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
        return null;
    }

    @Override
    public Vector times() {
        return null;
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
        return null;
    }

    @Override
    public Vector minus(Vector v) {
        return null;
    }

    @Override
    public Vector like() {
        return null;
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public Map<Integer, Double> get() {
        return null;
    }

    public double getValue(int index) {
        return this.values[index];
    }

    @Override
    public void set(int index, Double value) {
        this.values[index] = value;
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
}
