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
        return 0;
    }

    @Override
    public Map<Integer, Double> get() {
        return null;
    }

    @Override
    public void set(int index, Double value) {
        this.values[index] = value;
    }
}
