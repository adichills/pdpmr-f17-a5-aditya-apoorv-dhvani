// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySort {
    
    public static class KeyPartitioner extends Partitioner<MeanDelayCompositeKey, DoubleWritable>{
        @Override
        public int getPartition(MeanDelayCompositeKey key, DoubleWritable value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static class KeyComparator extends WritableComparator{
        public KeyComparator() {
            super (MeanDelayCompositeKey.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            MeanDelayCompositeKey key1 = (MeanDelayCompositeKey) w1;
            MeanDelayCompositeKey key2 = (MeanDelayCompositeKey) w2;
            return key1.compareTo(key2);
        }
    }

    public static class GroupComparator extends WritableComparator{
        public GroupComparator(){
            super(MeanDelayCompositeKey.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            MeanDelayCompositeKey key1 = (MeanDelayCompositeKey) w1;
            MeanDelayCompositeKey key2 = (MeanDelayCompositeKey) w2;
            return key1.compareTo(key2);
        }
    }
}
