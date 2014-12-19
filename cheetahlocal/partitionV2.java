
/**
 * partitionV2 for NewRowColStoreEng
 *
 * This class is different from "partition" class in that it store all data types in one array of the type long.
 *  String: store position (long) to the stringbuffer into the main array table
 *  Long: store as long
 *  Boolean: Store as long - True as 1 and false as 0
 *  Double: same as the SCUBA paper
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


public class partitionV2{
    protected long UNDEFINED= -1111111; //represent NULL
    protected String separator=":"; // special character to seprate key and type, use : for now
    
    //---------------- Meta-data that stores keys and object ids ------------------------//
    int numObject;
    int[] objectIds; //list of object ids stored in this partition
    String[] keys;
    
    //----------- Actual tables that store the data in this partition in memory -----------//
    long[] data;
    
    //partition constructor, given a partition definition and max object size, initialize tables and meta data
    public partitionV2(String partition, int max_size){
        //max_size is the maximum number of Objects, NOT max bytes
        keys = partition.split("\\s+"); //separted by any white space
        data = new long[max_size];
        objectIds= new int[max_size];
        numObject = 0;
    }
    
    public void insertObject(int objid, Hashtable<String, Long> tempLongs, Hashtable<String, Double> tempDoubles, Hashtable<String, String> tempStrings, Hashtable<String, String> tempBool, ByteBuffer stringBuffer){
        if(keys == null){
            System.out.println("please contruct this partition first.");
            return;
        }
        if(numObject == objectIds.length){
            System.out.println("Full. No inserts any more!");
            return;
        }
        
        boolean emptyRow = true;
        
        for(int i = 0; i < keys.length; i++){
            String[] parts = keys[i].split(separator);
            if(parts[1].equals("STRING")){
                if(tempStrings.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = stringBuffer.position();
                    stringBuffer.putInt(tempStrings.get(keys[i]).length());
                    stringBuffer.put(tempStrings.get(keys[i]).getBytes());
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }else if(parts[1].equals("LONG")){
                if(tempLongs.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = tempLongs.get(keys[i]);
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }else if(parts[1].equals("DOUBLE")){
                if(tempDoubles.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = (long)( tempDoubles.get(keys[i]) * 100000);
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }else if(parts[1].equals("BOOL")){
                if(tempBool.get(keys[i]) != null){
                    if(tempBool.get(keys[i]).equals("TRUE")){
                        data[(keys.length * numObject + i)] = (long)1;
                    }else if(tempBool.get(keys[i]).equals("FALSE")){
                        data[(keys.length * numObject + i)] = (long)0;
                    }
                    emptyRow = false;
                }else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
        }
        
        if(emptyRow == false){
            objectIds[numObject] = objid;
            numObject++;
        }
        return;
    }
    
    public void getObjectFromPartition(int objid){
        for(int i = 0; i < numObject; i++){
            if(objectIds[i] > objid){
                return;
            }
            if(objectIds[i] == objid){
                //TODO:
            }
        }
        return;
    }
    
    public void select(byte[][] columns, ResultSetV2 result){
        boolean[] access = new boolean[keys.length];
        boolean haveFields = false;
        for(int i = 0; i < keys.length; i++){
            access[i] = false;
            for(int j = 0; j < columns.length; j++){
                String key = keys[i].split(separator)[0];
                if(key.equals( new String(columns[j]) )){
                    access[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keys.length;
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            for(int j = 0; j < access.length; j++){
                if(access[j]){
                    long longnum = data[i*numOfFields + j];
                    String key = keys[j];
                    if(longnum != UNDEFINED)
                        result.addLong(key, oid, longnum);
                }
            }
        }
        return;
    }
    
    public void selectCondition(List<Integer> oidList, byte[][] columns, ResultSetV2 result){
        boolean[] access = new boolean[keys.length];
        boolean haveFields = false;
        for(int i = 0; i < keys.length; i++){
            access[i] = false;
            for(int j = 0; j < columns.length; j++){
                String key = keys[i].split(separator)[0];
                if(key.equals( new String(columns[j]) )){
                    access[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keys.length;
        int index = 0;
        int targetId = oidList.get(index);
        
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            
            if(oid == targetId){
                for(int j = 0; j < access.length; j++){
                    if(access[j]){
                        long num = data[i*numOfFields + j];
                        String key = keys[j];
                        if(num != UNDEFINED)
                            result.addLong(key, oid, num);
                    }
                }
            }else if(oid > targetId){
                i--;
                index++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break;
            }
        }
        return;
    }
    
    //this is the partition with where field
    public List<Integer> selectRangeWherePar(byte[][] selectCols, byte[] whereCol, long value1, long value2, ResultSetV2 result){
        List<Integer> oidList = new ArrayList<Integer>();
        
        //assuming the where col is in type LONG
        boolean[] access = new boolean[keys.length];
        int whereIndex = -1;
        
        for(int i = 0; i < keys.length; i++){
            String key = keys[i].split(separator)[0];
            if(key.equals( new String(whereCol) )) whereIndex = i;
        
            access[i] = false;
            for(int j = 0; j < selectCols.length; j++){
                if(key.equals( new String(selectCols[j]) )){
                    access[i] = true;
                    break;
                }
            }
        }
        
        int numOfFields = keys.length;
        for(int i = 0; i < numObject; i++){
            //first check if this object meet the where condition
            long value = data[i*numOfFields + whereIndex];
            if( (value >= value1) && (value <= value2)){ //this object meets condition
                int oid=objectIds[i];
                oidList.add(oid);
                //select fields in the select clause
                for(int j = 0; j < access.length; j++){
                    if(access[j]){
                        String key = keys[j];
                        if(j == whereIndex){
                            result.addLong(key, oid, value);
                            continue;
                        }
                        long longnum = data[i*numOfFields + j];
                        if(longnum != UNDEFINED)
                            result.addLong(key, oid, longnum);
                    }
                }
            }
        }
        return oidList;
    }
}
