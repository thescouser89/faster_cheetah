
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

public class ResultSet{
    protected int  UNDEFINED= -1111111; //represent NULL
    protected byte [] UndefByte= new byte[1];
    
    /* special character to seprate key and type, use : for now*/
    protected String separator=":";
    
    public long[] longValues;
    public long[] stringValues;
    public double[] doubleValues;
    public byte[] booleanValues;
    public int index;
    public int[] oids;
    public String[] keys;
    
    public ResultSet(int size){
        stringValues = new long[size];
        longValues = new long[size];
        doubleValues = new double[size];
        booleanValues = new byte[size];
        index = 0;
        oids = new int[size];
        keys = new String[size];
        UndefByte[0] = -1;

    }
    
    public void addString(String key, int oid, long pos){
        stringValues[index] = pos;
        longValues[index] = UNDEFINED;
        doubleValues[index] = UNDEFINED;
        booleanValues[index] = UndefByte[0];
        keys[index] = key;
        oids[index] = oid;
        index++;
    }
    
    public void addLong(String key, int oid, long longnum){
        longValues[index] = longnum;
        doubleValues[index] = UNDEFINED;
        doubleValues[index] = UNDEFINED;
        booleanValues[index] = UndefByte[0];
        keys[index] = key;
        oids[index] = oid;
        index++;
    }
    
    public void addDouble(String key, int oid, double num){
        stringValues[index] = UNDEFINED;
        longValues[index] = UNDEFINED;
        doubleValues[index] = num;
        booleanValues[index] = UndefByte[0];
        keys[index] = key;
        oids[index] = oid;
        index++;
    }
    
    public void addBool(String key, int oid, byte bool){
        stringValues[index] = UNDEFINED;
        longValues[index] = UNDEFINED;
        doubleValues[index] = UNDEFINED;
        booleanValues[index] = bool;
        keys[index] = key;
        oids[index] = oid;
        index++;
    }
    
    public void clearResultSet(){ index = 0; }
    
    //return all selected objects in a HashMap
    public HashMap<Integer, String> selectObjects(){
        HashMap<Integer, String> resultSet = new HashMap<Integer, String>();
        for(int i = 0; i < index; i++){
            if(longValues[i] != UNDEFINED)
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(longValues[i]));
            else if(stringValues[i] != UNDEFINED)
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(stringValues[i]));
            else if(doubleValues[i] != UNDEFINED)
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(doubleValues[i]));
            else if(booleanValues[i] != UndefByte[0])
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(booleanValues[i]));
        }
        return resultSet;
    }
    
    public void printBriefStats(){
        HashMap<Integer, String> resultSet = new HashMap<Integer, String>();
        int longCount = 0;
        int stringCount = 0;
        int doubleCount = 0;
        int boolCount = 0;
        int other = 0;
        int trueCount = 0;
        int falseCount = 0;
        for(int i = 0; i < index; i++){
            if(longValues[i] != UNDEFINED){
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(longValues[i]));
                longCount++;
            }else if(stringValues[i] != UNDEFINED){
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(stringValues[i]));
                stringCount++;
            }
            else if(doubleValues[i] != UNDEFINED){
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(doubleValues[i]));
                doubleCount++;
            }
            else if(booleanValues[i] != UndefByte[0]){
                resultSet.put(oids[i], keys[i]+separator+String.valueOf(booleanValues[i]));
                boolCount++;
                /*if(booleanValues[i] == (byte)0 ){
                    trueCount++;
                }
                if(booleanValues[i] == (byte)1 ){
                    falseCount++;
                }*/
            }else{
                //other++;
            }
            
        }
        System.out.println("selected: " + resultSet.size() + " objects with " + index + " fields - " + stringCount + " strings, " + longCount + " longs, " + doubleCount + " doubles, " + boolCount + " booleans ");
        return;
    }
}
