package huyang.edu.cn;

import java.util.HashMap;
import java.util.Map;

/**
 * 稀疏向量
 */
public class sampleVector implements Vector<Double>{
    private Map<Integer,Double> values;

    private int classify = 0;

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
    public int getSize() {
        return size;
    }

    @Override
    public Double get(int index) {
        return this.values.get(index);
    }

    public void setSize(int size) {
        this.size = size;
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
        sampleVector vector = (sampleVector) x;
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),entry.getValue()+vector.get(entry.getKey()));
        }
        return result;
    }
    @Override
    public Double zSum() {
        double result =0;
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result += entry.getValue();
        }
        return result;
    }

    @Override
    public Vector clone() {
        Map<Integer,Double> data = new HashMap<Integer, Double>();
        data.putAll(this.values);
        return new sampleVector(this.size,data);
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
        assert (this.size == v.getSize());
        sampleVector vector = (sampleVector) v;
        sampleVector result = new sampleVector();
        for(int i=0; i < this.size; i++) {
            result.set(i,get(i)-((sampleVector) v).get(i));
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

    public void clean() {
        this.values.clear();
        this.size = 0;
    }

    @Override
    public Vector square() {
        sampleVector result = new sampleVector();
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            result.set(entry.getKey(),Math.sqrt(entry.getValue()));
        }
        return result;
    }

    public DenseVector toDenseVector() {
        DenseVector denseVector = new DenseVector(this.size);
        int index = 0;
        for(Map.Entry<Integer, Double> entry : this.values.entrySet()) {
            denseVector.set(index++,entry.getValue());
        }
        return denseVector;
    }

    public String toString() {
        String str=" ";
        for(int i=0;i<this.size;i++) {
            str+=get(i).toString()+" ";
        }
        return "["+str+"]";
    }
}
