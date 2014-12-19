/**
 * An row-based buffer store  
 * @author Jin Chen, Alan Lu
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.HashMap;
import java.io.BufferedReader;

public class NewRowStoreEng extends StoreEngine {
    int numObject;
    int[] objectIds;
    String[] keys;
    String[] types;
    
    //----------- Actual tables that store the data in this partition in memory -----------//
    long[] data;
	
    private ByteBuffer stringBuffer;
    ResultSetV2 result;
    
    private static Hashtable <String, Long> tempBufLong = new Hashtable<String, Long>(); 
    private static Hashtable <String, Double> tempBufDouble = new Hashtable<String, Double>(); 
    private static Hashtable <String, String> tempBufString = new Hashtable<String, String>();
    private static Hashtable <String, String> tempBufBool = new Hashtable<String, String>();
    
    
    public NewRowStoreEng (int memory_size_in_bytes, String defFile)
    {
		super(memory_size_in_bytes);

        stringBuffer = ByteBuffer.allocateDirect(max_buf_size/5);
        stringBuffer.position(0);
		result = new ResultSetV2(100*1000*1000);
        data = new long[memory_size_in_bytes/8];
        objectIds = new int[memory_size_in_bytes/8/1000];
        
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(defFile));
            String line  = bufferedReader.readLine();
			String[] temp = line.split("\\s+"); //separated by space
            
			keys = new String[temp.length];
			types = new String[temp.length];
			for(int i=0; i<temp.length; i++){
				String[] temp2 = temp[i].split(":");
				keys[i] = temp[i];
				types[i] = temp2[1];
			}
            bufferedReader.close();
        } catch (FileNotFoundException e){
                        System.err.println("FileNotFoundException:"+e.getMessage());
                        return ;
        } catch (IOException e){
                        System.err.println("IOException:"+e.getMessage());
                        return;
        }
	}
    
    public void insertObject(int objid, JsonValue tree, String key){
		tempBufLong.clear();
		tempBufDouble.clear();
		tempBufString.clear();
        tempBufBool.clear();
		prepareRow(objid, tree, key);
		insertRow(objid);
	}
    
    public void prepareRow(int objid, JsonValue tree, String key){
        switch(tree.getValueType()){
            case OBJECT:
                JsonObject object = (JsonObject) tree;
                for(String name: object.keySet()){
                    if(key!=null)
                        prepareRow(objid,object.get(name),key+"."+name);
                    else
                        prepareRow(objid,object.get(name),name);
                }
                break;
            case ARRAY:
                JsonArray array = (JsonArray) tree;
                int index =0;
                for (JsonValue val : array){
                    prepareRow(objid,val,key+"["+index+"]");
                    index += 1;
                }
                break;
            case STRING:
                JsonString st = (JsonString) tree;
                tempBufString.put(key+separator+"STRING", st.getString());
                break;
            case NUMBER:
                JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
                    tempBufLong.put(key+separator+"LONG", num.longValue());
                }else{
                    tempBufDouble.put(key+separator+"DOUBLE", num.doubleValue());
                }
                break;
            case TRUE:
            case FALSE:
                tempBufBool.put(key+separator+"BOOL", tree.getValueType().toString());
                break;
            case NULL:
                break;
        }
	}
    
	public void insertRow(int objid){
		if(numObject == objectIds.length){
            System.out.println("Full. No inserts any more!");
            return;
        }
        
		for(int i = 0; i < keys.length; i++){
            String type = types[i];
            if(type.equals("STRING")){
				if(tempBufString.get(keys[i]) != null){
					long position = stringBuffer.position();
                    data[(keys.length * numObject + i)] = position;
					stringBuffer.putInt(tempBufString.get(keys[i]).length());
    				stringBuffer.put(tempBufString.get(keys[i]).getBytes());
				}
    			else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
            else if(type.equals("LONG")){
    			if(tempBufLong.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = tempBufLong.get(keys[i]);
                }
    			else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
            else if(type.equals("DOUBLE")){
            	if(tempBufDouble.get(keys[i]) != null){
                    data[(keys.length * numObject + i)] = (long)( tempBufDouble.get(keys[i]) * 100000);
                }
    			else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
            else if(type.equals("BOOL")){
                if(tempBufBool.get(keys[i]) != null){
                    String value = tempBufBool.get(keys[i]);
                    if(value.equals("TRUE")==true){
                        data[(keys.length * numObject + i)] = (long)1;
                    }else if(value.equals("FALSE")==true){
                        data[(keys.length * numObject + i)] = (long)0;
                    }
                }
                else{
                    data[(keys.length * numObject + i)] = UNDEFINED;
                }
            }
            else{
                System.out.println("Error: no such type in buf "+type);
                return;
            }
		}
        objectIds[numObject] = objid;
        numObject++;
        
        return;
	}

    
    public int getRow(int targetId)
	{
		//TODO:
        return 0;
	}
   
    
	private int skipNext(ByteBuffer readBuf, int position, String type){
        int len=0;
		if(type.equals("STRING")){
			readBuf.position(position);
            len=readBuf.getInt();
			readBuf.position(position);
            position += 4;
			if(len != UNDEFINED){
            	position += len;
				//System.out.print(len+"-");
            }
		} else if(type.equals("LONG")){
			position += 8;
		} else if(type.equals("DOUBLE")){
			position += 8;
		} else if(type.equals("BOOL")){
			position += 1;
		}
		return position;
	}
    
    public void select(byte[][] columns){
        result.clearResultSet();
        
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
    
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2){
        //clear the result data structure that contains selected data in memory
        result.clearResultSet();
        
        String where = (new String(whereCol)) + separator + "LONG";
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
                //oidList.add(oid);
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
        
        return;
    }

	/* TODO: Finish this
     * select where value = ANY xxx
     *  xxx is a set / JASON array, need to compare each member of this set
     */
	public HashMap<Integer, HashMap<String, String>> selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
        return resultSet;
	}
    
    /* TODO: Finish this
     *select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
        return resultSet;
    }
    
    public void printBriefStats(){
        result.printBriefStats();
        return;
    }
    
	public static void main(String[] args) throws IOException{
	}
}
