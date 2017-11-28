package huyang.edu.cn;

import java.util.Arrays;

public class DenseVector implements Vector<Double> {
    private double[] values;

    private int classify = 0;

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
    public int getClassify() {
        return this.classify;
    }

    @Override
    public void setClassify(int classify) {
        this.classify = classify;
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
    public Double zSum() {
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
        assert (this.size == vector.getSize());
        DenseVector denseVector = new DenseVector(this.size);
        for(int i=0; i<this.size;i++) {
            denseVector.set(i,get(i)*(Double)vector.get(i));
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
    public Vector minus(Vector<Double> v) {
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
    public Double get(int index) {return this.values[index]; }

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

    @Override
    public Vector square() {
        DenseVector denseVector = new DenseVector(this.size);
        for(int i=0; i<this.size; i++) {
            denseVector.set(i,Math.sqrt(this.values[i]));
        }
        return denseVector;
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

    public sampleVector  ToSampleVector() {
        sampleVector samplevector = new sampleVector();
        for(int i=0; i<this.size; i++) {
            samplevector.set(i,this.values[i]);
        }
        return samplevector;
    }

    public String toString() {
        String str=" ";
        for(int i=0;i<this.size;i++) {
            str+=get(i).toString()+" ";
        }
        return "["+str+"]";
    }
}
