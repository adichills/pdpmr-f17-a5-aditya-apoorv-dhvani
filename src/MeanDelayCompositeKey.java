// @Author : Apoorv Anand , Aditya Kammardi Sathyanarayan ,Dhvani Sheth

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// this class holds the composite key comprising year, month, airport and airline
// so that we can get mean delay based on this
public class MeanDelayCompositeKey implements Writable, 
    WritableComparable<MeanDelayCompositeKey> {
    IntWritable year;
    IntWritable month;
    Text origin;
    Text dest;
    Text airline;

    public MeanDelayCompositeKey() {
        set(new IntWritable(0), new IntWritable(0), new Text(), new Text(), 
            new Text());
    }
    
    public MeanDelayCompositeKey(int year, int month, String origin, String dest, 
        String airline) {
        set(new IntWritable(year), new IntWritable(month), new Text(origin), 
            new Text(dest), new Text(airline));
    }

    public void set(IntWritable year, IntWritable month, Text origin, Text dest, 
        Text airline) {
        this.year = year;
        this.month = month;
        this.origin = origin;
        this.dest = dest;
        this.airline = airline;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }
    
    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }
    
    public Text getOrigin() {
        return origin;
    }
    
    public void setOrigin(Text origin) {
        this.origin = origin;
    }

    public Text getDestination() {
        return dest;
    }
    
    public void setDestination(Text dest) {
        this.dest = dest;
    }
    
    public Text getAirline() {
        return airline;
    }
    
    public void setAirline(Text airline) {
        this.airline = airline;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        month.write(out);
        dest.write(out);
        airline.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        month.readFields(in);
        dest.readFields(in);
        airline.readFields(in);
    }

    @Override
    public int compareTo(MeanDelayCompositeKey other) {
        // compare by year, month, airport and airline
        int cmp = this.year.compareTo(other.getYear());
        if (cmp != 0) {
            return cmp;
        }
        cmp = this.month.compareTo(other.getMonth());
        if (cmp != 0) {
            return cmp;
        }
        cmp = this.origin.compareTo(other.getOrigin());
        if (cmp != 0) {
            return cmp;
        }
        cmp = this.dest.compareTo(other.getDestination());
        if (cmp != 0) {
            return cmp;
        }
        cmp = this.airline.compareTo(other.getAirline());
        if (cmp != 0) {
            return cmp;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((airline == null) ? 0 : airline.hashCode());
        result = prime * result + ((dest == null) ? 0 : dest.hashCode());
        result = prime * result + ((month == null) ? 0 : month.hashCode());
        result = prime * result + ((origin == null) ? 0 : origin.hashCode());
        result = prime * result + ((year == null) ? 0 : year.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MeanDelayCompositeKey other = (MeanDelayCompositeKey) obj;
        if (airline == null) {
            if (other.airline != null)
                return false;
        } else if (!airline.equals(other.airline))
            return false;
        if (dest == null) {
            if (other.dest != null)
                return false;
        } else if (!dest.equals(other.dest))
            return false;
        if (month == null) {
            if (other.month != null)
                return false;
        } else if (!month.equals(other.month))
            return false;
        if (origin == null) {
            if (other.origin != null)
                return false;
        } else if (!origin.equals(other.origin))
            return false;
        if (year == null) {
            if (other.year != null)
                return false;
        } else if (!year.equals(other.year))
            return false;
        return true;
    }
}
