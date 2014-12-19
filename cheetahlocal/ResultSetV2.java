
/**
 * ResultSet
 *
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class ResultSetV2{
    protected long UNDEFINED= -1111111; //represent NULL
    
    /* special character to seprate key and type, use : for now*/
    protected String separator=":";
    
    public long[] longValues;
    public int index;
    public int[] oids;
    public String[] keys;
    public int size;
    
    public ResultSetV2(int size){
        this.longValues = new long[size];
        this.index = 0;
        this.oids = new int[size];
        this.keys = new String[size];
        this.size = size;
    }
    
    public void addLong(String key, int oid, long longnum){
        longValues[index] = longnum;
        keys[index] = key;
        oids[index] = oid;
        index++;
    }
    
    public void clearResultSet(){ index = 0; }
    
    //return all selected objects in a HashMap
    public HashMap<Integer, String> selectObjects(){
        //TODO:
        return null;
    }
    
    public void printBriefStats(){
        HashMap<Integer, String> resultSet = new HashMap<Integer, String>();
        for(int i = 0; i < index; i++){
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(longValues[i]));
        }
        System.out.print("selected " + resultSet.size() + " objects with " + index + " fields");
        return;
    }
    
    public String makeBriefStatsString() {
        HashMap<Integer, String> resultSet = new HashMap<Integer, String>();
        for(int i = 0; i < index; i++){
                resultSet.put(oids[i], "a"/*keys[i]+separator+String.valueOf(longValues[i])*/);
        }
       return new String("selected " + resultSet.size() + " objects with " + index + " fields");
    }
}
