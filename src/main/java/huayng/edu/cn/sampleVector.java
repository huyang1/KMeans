package huayng.edu.cn;

import java.util.HashMap;
import java.util.Map;

/**
 * 稀疏向量
 */
public class sampleVector implements Vector{
    private Map<Integer,Double> values;

    private int size;

    public sampleVector() {
        values = new HashMap<Integer,Double>();
        size = 0;
    }


    public sampleVector(int size, Map<Integer, Double> map) {
        this.size = size;
        this.values = map;
    }

    @Override
    public void set(int index ,Double value) {
        values.put(index,value);
        size++;
    }

    @Override
    public Map<Integer, Double> get() {
        return values;
    }

    @Override
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public Vector plus(double x) {
        return null;
    }

    @Override
    public Vector plus(Vector x) {
        sampleVector vector = (sampleVector) x;
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),entry.getValue()+vector.get().get(entry.getKey()));
        }
        return result;
    }
    @Override
    public double zSum() {
        double result =0;
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result += entry.getValue();
        }
        return result;
    }

    @Override
    public Vector clone() {
        return new sampleVector(this.size, this.values);
    }

    @Override
    public Vector times() {
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),entry.getValue()*entry.getValue());
        }
        return result;
    }

    @Override
    public Vector divide(double x) {
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),entry.getValue()/x);
        }
        return result;
    }

    @Override
    public Vector multi(double x) {
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),entry.getValue()*x);
        }
        return result;
    }

    @Override
    public Vector minus(Vector v) {
        sampleVector vector = (sampleVector) v;
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),entry.getValue()-vector.get().get(entry.getKey()));
        }
        return result;
    }

    @Override
    public Vector like() {
        sampleVector vector = new sampleVector();
        for(int i=0;i<this.size;i++) {
            vector.set(i,0d);
        }
        return vector;
    }
}
