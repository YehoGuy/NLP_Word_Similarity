package Step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class Map2 implements Writable{

    private final MapWritable map;

    public Map2(MapWritable map){
        this.map = map;
    }

    public Map2(){
        this.map = new MapWritable();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for(Writable key : map.keySet()){
            sb.append(key.toString());
            sb.append(":");
            sb.append(map.get(key).toString());
            sb.append(" , ");
        }
        sb.append("}");
        return sb.toString();
    }

    public MapWritable getMap(){
        return map;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        map.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        map.readFields(in);
    }
    
}
