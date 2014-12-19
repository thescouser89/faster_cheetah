
/**
 * Save JSON file into memory based column store 
 * Every JSON field is saved into a column, represented by a buffer    
 * 
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
 *
 * @author Jin Chen, Alan Lu
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;

public class ColStoreEng extends StoreEngine  {


    /* key+type as the key for the hash table */
    Hashtable<String, ByteBuffer> colBufs;
    ResultSet result;
    
	private ByteBuffer stringBuffer;

    /**
     * Creates a ring buffer, and using Column format to store the data
     * initialize the buffer size 
     */
    public ColStoreEng(int memory_size_in_bytes)
    {
        //call parent constructor
        super(memory_size_in_bytes);
        /* create colum Buffer hash table */ 
        colBufs = new Hashtable<String, ByteBuffer>();  
		stringBuffer = ByteBuffer.allocateDirect(max_buf_size*10);

        result = new ResultSet(100*1000*1000);
    }

    /**
     * objid - int, 4Bytes
     * valstr = length int + chars, 4 Bytes + length
     */

    protected int saveStrValue(int objid, String key, String value)
    {
        /* check whether we have buffer or not */

        //later -- use dictionary to compress
        // save to str
        String bufkey = key+separator+"STRING";
        ByteBuffer buf=colBufs.get(bufkey);
        if(buf == null){
            // allocate the byte buffer 
            //System.out.println("allocate buf "+bufkey+" "+max_buf_size);
            
            //Temporary - for running sparse data on nobench_data.json
            if(key.matches("sparse_")){
                //buf = ByteBuffer.allocateDirect(max_buf_size/1000);
                buf = ByteBuffer.allocateDirect(2000);
                colBufs.put(bufkey,buf);
                return 1;
            }
            
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        // insert content to buf  - no need to save key since it is in hash table
        
        if(buf != null){
            // insert  
            buf.putInt(objid);
			long position = stringBuffer.position();
			buf.putLong(position);
			stringBuffer.putInt(value.length());
			stringBuffer.put(value.getBytes());
            //buf.putInt(value.length());
            //buf.put(value.getBytes());
        }
        
        return 1;
        
    }

    protected int saveLongValue(int objid, String key, long num)
    {
        String bufkey = key+separator+"LONG";
        ByteBuffer buf=colBufs.get(bufkey);
        if(buf == null){
            //allocate byte buffer
            //System.out.println("allocate buf "+bufkey+" "+max_buf_size);
            
            //Temporary - for running sparse data on nobench_data.json
            if(key.matches("sparse_")){
                buf = ByteBuffer.allocateDirect(max_buf_size/100000);
                colBufs.put(bufkey,buf);
                return 1;
            }
            
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        if(buf != null){
            buf.putInt(objid);
            buf.putLong(num);
        }
        return 1;
    } 

    protected int saveDoubleValue(int objid, String key, double num)
    {
        String bufkey = key+separator+"DOUBLE";
        ByteBuffer buf=colBufs.get(bufkey);
        if(buf == null){
            //allocate byte buffer
            //System.out.println("allocate buf "+bufkey+" "+max_buf_size);
            
            //Temporary - for running sparse data on nobench_data.json
            if(key.matches("sparse_")){
                buf = ByteBuffer.allocateDirect(max_buf_size/100000);
                colBufs.put(bufkey,buf);
                return 1;
            }
            
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        if(buf !=null){
            buf.putInt(objid);
            buf.putDouble(num);
        }
        return 1;
    } 

    protected int saveBoolValue(int objid, String key, String value)
    {
        String bufkey = key+separator+"BOOL";
        ByteBuffer buf = colBufs.get(bufkey);
        if(buf == null){
            //System.out.println("allocate buf "+bufkey+" "+max_buf_size);
            
            //Temporary - for running sparse data on nobench_data.json
            if(key.matches("sparse_")){
                buf = ByteBuffer.allocateDirect(max_buf_size/100000);
                colBufs.put(bufkey,buf);
                return 1;
            }
            
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        if(buf !=null ){
            buf.putInt(objid);
            if(value.equals("TRUE")==true){
                buf.put((byte)1);
            }else if(value.equals("FALSE")==true){
                buf.put((byte)0);
            }else{
                System.out.println("Error: unknow value "+value);
            }
        }
        return 1;
    }
    

    /**
     * Fixed row format [objid,keystr,valstr,valnum,valbool] 
     */

    public int getObject(int targetId)
    {
        /* tranverse each buffer to read it */
        //get the buffer
        System.out.println("Iterating on the buffer hashtable"+colBufs.keySet());
        for(String key: colBufs.keySet()){
            //System.out.println("key:"+key);
            // check the key and assign different method
            ByteBuffer buf = colBufs.get(key);
            String [] parts = key.split(separator); 
            String columnKey = parts[0];
            String type = parts[1];
            if(type.equals("STRING")){
                //System.out.println("STRING "+columnKey);
                getObjectFromStrBuf(buf,targetId);
            }else if(type.equals("LONG")){
                //System.out.println("LONG "+columnKey);
                getObjectFromLongBuf(buf,targetId);
            }else if(type.equals("DOUBLE")){
                //System.out.println("DOUBLE "+columnKey);
                getObjectFromDoubleBuf(buf,targetId);
            }else if(type.equals("BOOL")){
                //System.out.println("BOOL "+columnKey);
                getObjectFromBoolBuf(buf,targetId);
            }else{
                System.out.println("Error: no such type in buf "+type);
                return 1;
            }
        }

        return 0;
    }

    public void getObjectFromBoolBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read bool   
            byte [] valbool = new byte[1];
            readBuf.get(valbool);
            if(oid == targetId)
                System.out.println(" " + valbool[0]);
        }
    }

    public void getObjectFromDoubleBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read double  
            double value = readBuf.getDouble();
            if(oid == targetId)
                System.out.println(" " + value);
        }
    }

    public long getObjectFromLongBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read Long  
            long value = readBuf.getLong();
            if(oid == targetId){
                System.out.println(" " + value);
                return value;
            }
        }
        return UNDEFINED;

    }


    public void getObjectFromStrBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
		ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
        readBuf.position(0);
		stringReadBuf.position(0);
        int oid = 0;
        byte [] key,valstr; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read val string
			int pos = (int)readBuf.getLong();
			if(pos == UNDEFINED){
				continue;
			}
			stringReadBuf.position(pos);
			len = stringReadBuf.getInt();
//            len = readBuf.getInt();
            valstr = new byte[len];
//            readBuf.get(valstr);
			stringReadBuf.get(valstr);
            if(oid == targetId)
                System.out.println(" " + new String(valstr));
        }

    }

    /**
    *
    Table 1: objid + keystr (4B + 4B + length)
    Table 2: objid + valstr (4B + 4B + length)
    Table 3: objid + INT   (4B+4B - int array)
    Talbe 4: objid + LONG (4B + 8B
    Table 5: objid + DOUBLE
    Table 6: objid + BOOL ( 4B + 1B )  -- maybe compressed with objid together
    *
    */

    /**
    * Navigate the json object and parse it and save it into storage 
    *
    */

    public void insertObject(int objid, JsonValue tree, String key){

        switch(tree.getValueType()){
            case OBJECT:
                //System.out.println("  OBJECT");
                JsonObject object = (JsonObject) tree;
                for(String name: object.keySet()){
                    if(key!=null)
                        insertObject(objid,object.get(name),key+"."+name);
                    else
                        insertObject(objid,object.get(name),name);
                }
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
                break;
            case ARRAY:
                //System.out.println("  ARRAY");
                JsonArray array = (JsonArray) tree;
                int index =0; 
                for (JsonValue val : array){
                    insertObject(objid,val,key+"["+index+"]");
                    index += 1;
                }
                break;
            case STRING:
                JsonString st = (JsonString) tree;
                saveStrValue(objid,key,st.getString());
                //System.out.println(objid+" "+key+" "+st.getString());
                break;
            case NUMBER:
                JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
                    saveLongValue(objid,key,num.longValue());
                }else{
                    saveDoubleValue(objid,key,num.doubleValue());
                }
                //System.out.println(objid+" "+key+" "+num.toString());
                break;
            case TRUE:
            case FALSE:
                saveBoolValue(objid,key,tree.getValueType().toString());
                //System.out.println(objid+" "+key+" "+tree.getValueType().toString());
                break;
            case NULL:
                // we didn't save null value
                //System.out.println("null\n:");
                //System.out.println(objid+"key "+key+tree.getValueType().toString());
                break;
        }


    }

  

   /*
    *    Given a list of oids, select the corresponding columns which match the oids and put them into results
    *    Return a result set
    *    Called by select where executor
    */

    protected void selectCondition(List<Integer> oidList, byte[][] columns){
    	//	HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
        
        /* first handle select all case - later we need to optimize it to get values for each column  */
        if (oidList.size() == 0)
			return;
            //return resultSet;
        
        if(columns[0][0]==(byte) '*' ){
            //System.out.println("select all where");
            //tranverse all of columns
            for(String selectKey: colBufs.keySet()){
                //System.out.println("key:"+key);
                // check the key and assign different method
                ByteBuffer selectBuf = colBufs.get(selectKey);
                String [] parts = selectKey.split(separator);
                String columnKey = parts[0];
                String type = parts[1];
                if(type.equals("STRING")){
					selectConditionString(oidList, selectKey, selectBuf);
                    //System.out.println("STRING "+columnKey);
                    //resultSet = selectConditionString(oidList, selectKey, selectBuf, resultSet);
                }else if(type.equals("LONG")){
                    selectConditionLong(oidList,selectKey, selectBuf);
					//System.out.println("LONG "+columnKey);
                    //resultSet = selectConditionLong(oidList,selectKey, selectBuf, resultSet);
                }else if(type.equals("DOUBLE")){
					selectConditionDouble(oidList,selectKey, selectBuf);
                    //System.out.println("DOUBLE "+columnKey);
                    //resultSet = selectConditionDouble(oidList,selectKey, selectBuf, resultSet);
                }else if(type.equals("BOOL")){
					selectConditionBool(oidList,selectKey, selectBuf);
                    //System.out.println("BOOL "+columnKey);
                    //resultSet = selectConditionBool(oidList,selectKey, selectBuf, resultSet);
                }else{
                    System.out.println("Error: no such type in buf "+type);
                    break;
                }
            } //end for

            //return resultSet;
            return;
        }

        
        
        // tranverse colBufs,find the proper column buf
        for(int i = 0; i< columns.length; i++){
            ByteBuffer selectBuf;
            // since we don't know type, we try STRING ,LONG,DOUBLE,BOOL -- could be dyn type 
			String selectKey = new String(columns[i]) + separator + "STRING";
            selectBuf=colBufs.get(selectKey);
            
			if(selectBuf != null){
                //found the right type
				selectConditionString(oidList, selectKey, selectBuf);
//                resultSet = selectConditionString(oidList, selectKey, selectBuf, resultSet);
            }
			selectKey = new String(columns[i]) + separator + "LONG";
            selectBuf=colBufs.get(selectKey); 
			if(selectBuf != null){
                //found the right type
				selectConditionLong(oidList,selectKey, selectBuf);
                //resultSet = selectConditionLong(oidList,selectKey, selectBuf, resultSet);
            }
			selectKey = new String(columns[i]) + separator + "DOUBLE";
            selectBuf=colBufs.get(selectKey); 
			if(selectBuf != null){
                //found the right type
				selectConditionDouble(oidList,selectKey, selectBuf);
                //resultSet = selectConditionDouble(oidList,selectKey, selectBuf, resultSet);
               
            }
			selectKey = new String(columns[i]) + separator + "BOOL";
            selectBuf=colBufs.get(selectKey); 
			if(selectBuf != null){
                //found the right type  
                //resultSet = selectConditionBool(oidList,selectKey, selectBuf, resultSet);
                selectConditionBool(oidList,selectKey, selectBuf);
            }
        } //end for
        //return resultSet;
   	 	return;
    }
    
    /* 
     * selectConditionString 
     * select string type column results which match the oidList
     * Scan based approach
     * Note: we need to have index to speed it up
     */
    protected void selectConditionString(List<Integer> oidList, String selectKey, ByteBuffer selectBuf){

			//HashMap <String, String> innerResults;
			int bound = selectBuf.position();
			ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
			ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
            int oid=0;
            int len=0;
            byte[] valstr;
			readBuf.position(0);
            stringReadBuf.position(0);
            int index = 0;
            int targetId = oidList.get(index);
			while(readBuf.position()<bound){
            
                oid=readBuf.getInt();
                //check whether this oid is in the list 
                if(oid == targetId){
            	    //found one, add it into the result set
            	    //len=readBuf.getInt();
					int pos = (int)readBuf.getLong();
					result.addString(selectKey, oid, pos);
					/*
					stringReadBuf.position(pos);
					len = stringReadBuf.getInt();	
                    valstr = new byte[len];
                    //readBuf.get(valstr);
					stringReadBuf.get(valstr);
				    innerResults = resultSet.get(oid);
				    if(innerResults == null){
					    innerResults = new HashMap<String, String>();
				    }
				    innerResults.put(selectKey, new String(valstr));
				    resultSet.put(oid, innerResults);*/
            	    //update the targetId
            	    index ++;
            	    if(index < oidList.size())
            	        targetId = oidList.get(index);
            	    else
            	         break; // we get results for all of oids
                } else if(oid > targetId){
					index ++;
					if(index < oidList.size())
						targetId = oidList.get(index);
					else
						break;
					readBuf.position(readBuf.position() - 4);
				} else{
                	//skip this str
                	//len = readBuf.getInt();
                	//readBuf.position(readBuf.position()+ len);
					readBuf.position(readBuf.position() + 8);
                	
                }  
                
			}// while readBuf.position
        //return resultSet;
		return;
	}

    /*
     * selectConditionLong
     * select Long type column results which match the oidList
     * Scan based approach
     * Note: we need to have index to speed it up
     */
    protected void selectConditionLong(List<Integer> oidList, String selectKey, ByteBuffer selectBuf){
        
        //HashMap <String, String> innerResults;
        int bound = selectBuf.position();
        ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
        int oid=0;
        long longnum;
        
        readBuf.position(0);
        
        int index = 0;
        int targetId = oidList.get(index);
        while(readBuf.position()<bound){
            
            oid=readBuf.getInt();
            //check whether this oid is in the list
            if(oid == targetId){
                //found one, add it into the result set
                longnum = readBuf.getLong();
				result.addLong(selectKey, oid, longnum);
                /*innerResults = resultSet.get(oid);
                if(innerResults == null){
                    innerResults = new HashMap<String, String>();
                }
                innerResults.put(selectKey,  String.valueOf(longnum));
                resultSet.put(oid, innerResults);*/
                //update the targetId
                index ++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break; // we get results for all of oids
            } else if(oid > targetId){
					index ++;
					if(index < oidList.size())
						targetId = oidList.get(index);
					else
						break;
					readBuf.position(readBuf.position() - 4);
			} else{
                //skip this str
                readBuf.position(readBuf.position()+ 8); //long is 8 bytes
                
            }
            
        }// while readBuf.position
        //return resultSet;
		return;
	}
    /*
     * selectConditionDouble
     * select Double type column results which match the oidList
     * Scan based approach
     * Note: we need to have index to speed it up
     */
    protected void selectConditionDouble(List<Integer> oidList, String selectKey, ByteBuffer selectBuf){
        
        //HashMap <String, String> innerResults;
        int bound = selectBuf.position();
        ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
        int oid=0;
        double num;
        
        readBuf.position(0);
        
        int index = 0;
        int targetId = oidList.get(index);
        while(readBuf.position()<bound){
            
            oid=readBuf.getInt();
            //check whether this oid is in the list
            if(oid == targetId){
                //found one, add it into the result set
                num = readBuf.getDouble();
				result.addDouble(selectKey, oid, num);
                /*innerResults = resultSet.get(oid);
                if(innerResults == null){
                    innerResults = new HashMap<String, String>();
                }
                innerResults.put(selectKey,  String.valueOf(num));
                resultSet.put(oid, innerResults);*/
                //update the targetId
                index ++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break; // we get results for all of oids
            } else if(oid > targetId){
					index ++;
					if(index < oidList.size())
						targetId = oidList.get(index);
					else
						break;
					readBuf.position(readBuf.position() - 4);
				} else{
                //skip this str
                readBuf.position(readBuf.position()+ 8); //double is 8 bytes
                
            }
            
        }// while readBuf.position
        //return resultSet;
		return;
	}
    
    /*
     * selectConditionBool
     * select Bool type column results which match the oidList
     * Scan based approach
     * Note: we need to have index to speed it up
     */
    protected void selectConditionBool(List<Integer> oidList, String selectKey, ByteBuffer selectBuf){

		//HashMap <String, String> innerResults;
        int bound = selectBuf.position();
        ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
        int oid=0;
        int len=0;
        readBuf.position(0);
        byte [] valbool = new byte[1];
        
        int index = 0;
        int targetId = oidList.get(index);
        while(readBuf.position()<bound){
            
            oid=readBuf.getInt();
            //check whether this oid is in the list
            if(oid == targetId){
                //found one, add it into the result set
                readBuf.get(valbool);
				result.addBool(selectKey, oid, valbool[0]);
                /*innerResults = resultSet.get(oid);
                if(innerResults == null){
                    innerResults = new HashMap<String, String>();
                }
                innerResults.put(selectKey, String.valueOf(valbool[0]));
                resultSet.put(oid, innerResults);*/
                //update the targetId
                index ++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break; // we get results for all of oids
            } else if(oid > targetId){
                index ++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break;
                readBuf.position(readBuf.position() - 4);
            } else{
                //skip this str
                readBuf.position(readBuf.position()+ 1); //bool is 1 byte
                
            }
            
        }// while readBuf.position
        //return resultSet;
		return;
    }
    
    
    /*
     * fastSelect is faster than oldSelect, it selects each column individually
     * this select is a general select which can select any type 
     * prepare a result set in hashMap, and return it
     */   
    public void select(byte[][] columns){
		//HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
        // tranverse colBufs,find the proper column buf
        for(int i = 0; i< columns.length; i++){
            ByteBuffer selectBuf;
            // since we don't know type, we try STRING ,LONG,DOUBLE,BOOL -- could be dyn type 
			String selectKey = new String(columns[i]) + separator + "STRING";
            selectBuf=colBufs.get(selectKey);
            //HashMap <Integer, String> results = new HashMap<Integer, String>();
			if(selectBuf != null){
                //found the right type
 				selectString(selectKey, selectBuf);
                //resultSet = selectString(selectKey, selectBuf, resultSet);
                //tempResultSet.put(selectKey, results);
            }
			selectKey = new String(columns[i]) + separator + "LONG";
            selectBuf=colBufs.get(selectKey); 
			if(selectBuf != null){
                //found the right type
				selectLong(selectKey, selectBuf);
                //resultSet = selectLong(selectKey, selectBuf, resultSet);
                //tempResultSet.put(selectKey, results);
            }
			selectKey = new String(columns[i]) + separator + "DOUBLE";
            selectBuf=colBufs.get(selectKey); 
			if(selectBuf != null){
                //found the right type
				selectDouble(selectKey, selectBuf);
                //resultSet = selectDouble(selectKey, selectBuf, resultSet);
                //tempResultSet.put(selectKey, results);
            }
			selectKey = new String(columns[i]) + separator + "BOOL";
            selectBuf=colBufs.get(selectKey); 
			if(selectBuf != null){
                //found the right type 
				selectBool(selectKey, selectBuf);
                //resultSet = selectBool(selectKey, selectBuf, resultSet);
                //tempResultSet.put(selectKey, results);
            }
        } //end for
        //return resultSet;
		return;
    }

    protected void selectString(String selectKey, ByteBuffer selectBuf){

			//HashMap <String, String> innerResults;
			int bound = selectBuf.position();
			ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
			ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
            int oid=0;
            int len=0;
            byte[] valstr;
			readBuf.position(0);
			stringReadBuf.position(0);
			while(readBuf.position()<bound){
                oid=readBuf.getInt();
				int pos = (int)readBuf.getLong();
				result.addString(selectKey, oid, pos);
				/*stringReadBuf.position(pos);
				len = stringReadBuf.getInt();
                //len=readBuf.getInt();
                valstr = new byte[len];
				stringReadBuf.get(valstr);
                //readBuf.get(valstr);
				innerResults = resultSet.get(oid);
				if(innerResults == null){
					innerResults = new HashMap<String, String>();
				}
				innerResults.put(selectKey, new String(valstr));
				resultSet.put(oid, innerResults);*/
			}
        //return resultSet;
		return;
	}
/*
    protected HashMap<Integer, HashMap<String, String>> selectString(String selectKey, ByteBuffer selectBuf, HashMap<Integer, HashMap<String, String>> resultSet){
        
        HashMap <String, String> innerResults;
        int bound = selectBuf.position();
        ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
        ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
        int oid=0;
        int len=0;
        byte[] valstr;
        readBuf.position(0);
        stringReadBuf.position(0);
        
        while(readBuf.position()<bound){
            oid=readBuf.getInt();
            //len=readBuf.getInt();
            int pos = (int)readBuf.getLong();
            stringReadBuf.position(pos);
            len = stringReadBuf.getInt();
            valstr = new byte[len];
            //readBuf.get(valstr);
            stringReadBuf.get(valstr);
            innerResults = resultSet.get(oid);
            if(innerResults == null){
                innerResults = new HashMap<String, String>();
            }
            innerResults.put(selectKey, new String(valstr));
            resultSet.put(oid, innerResults);
        }
        return resultSet;
	}*/
    protected void selectLong(String selectKey, ByteBuffer selectBuf){
        //HashMap <String, String> innerResults;
        int bound = selectBuf.position();
        long num = 0;
        int oid = 0;
        ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
        readBuf.position(0);
        while(readBuf.position()<bound){
            oid=readBuf.getInt();
            num = readBuf.getLong();
			result.addLong(selectKey, oid, num);
            /*innerResults = resultSet.get(oid);
            if(innerResults == null){
                innerResults = new HashMap<String, String>();
            }
            innerResults.put(selectKey, String.valueOf(num));
            resultSet.put(oid, innerResults);*/
        }
        //return resultSet;
		return;
    }
    
    protected void selectDouble(String selectKey, ByteBuffer selectBuf){
			//HashMap <String, String> innerResults;
			int bound = selectBuf.position();
            double num = 0;
            int oid = 0;
			ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
			readBuf.position(0);
			while(readBuf.position()<bound){
                oid=readBuf.getInt();
				num = readBuf.getDouble();
				result.addDouble(selectKey, oid, num);
				/*innerResults = resultSet.get(oid);
				if(innerResults == null){
					innerResults = new HashMap<String, String>();
				}
				innerResults.put(selectKey, String.valueOf(num));
				resultSet.put(oid, innerResults);*/
			}
        //return resultSet;
		return;
    }

    protected void selectBool(String selectKey, ByteBuffer selectBuf){
		//HashMap <String, String> innerResults;
			int bound = selectBuf.position();
            int oid = 0;
			ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
			readBuf.position(0);
            byte [] valbool = new byte[1];
			while(readBuf.position()<bound){
                oid=readBuf.getInt();
                readBuf.get(valbool);
				result.addBool(selectKey, oid, valbool[0]);				
				
				/*innerResults = resultSet.get(oid);
				if(innerResults == null){
					innerResults = new HashMap<String, String>();
				}
				innerResults.put(selectKey,String.valueOf( valbool[0]));
				resultSet.put(oid, innerResults);*/
			}
        //return resultSet;
		return;
    }
 
    
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2)
    {
		result.clearResultSet();

        //HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
        ByteBuffer whereBuf;
        List<Integer> oidList = new ArrayList<Integer>();
        String whereKey = new String(whereCol)+separator+"LONG";
        whereBuf = colBufs.get(whereKey);
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey);
            //return resultSet;
			return;
        }
        ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
        whereReadBuf.position(0);
        int wbound = whereBuf.position();
        long value;
        boolean conditionFlag;
        int oid,len;
        while (whereReadBuf.position()<wbound){
            conditionFlag = false;
            oid = whereReadBuf.getInt();
            value = whereReadBuf.getLong();
            if( (value >= value1) && (value <= value2)){
                conditionFlag = true;
            }
            if(conditionFlag){
                // found it
                //System.out.println("found oid "+oid+ "results "+ value);
                oidList.add(oid);
            }
            
        }//while
        selectCondition(oidList,selectCols);
        //resultSet = selectCondition(oidList,selectCols);
		//return resultSet;
        return;
    }
    /* select x,y,z where a = ANY xx
     * xx is a set, single column and single relation parsing
     * Method:  there are multiple columns for this set xx,
     *         scan all of where columns and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     *        assume string type for now
     */
    public HashMap<Integer, HashMap<String, String>>  selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
        /*
        ByteBuffer whereBuf;
		ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
		stringReadBuf.position(0);
        List<Integer> oidList = new ArrayList<Integer>();
        // scan each related column xx[0],xx[1],...
        int count = 0;
        while(true){
            String whereKey = new String(whereCol)+"["+String.valueOf(count)+"]"+separator+"STRING";
            whereBuf = colBufs.get(whereKey);
            if(whereBuf == null){
                //System.out.println("Didn't find buffer key"+whereKey);
                break ; //no more columns for this set
            }
            count ++;
            ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
            whereReadBuf.position(0);
            int wbound = whereBuf.position();
            byte[] valstr;
            byte[] conditionStr = value;
           
            boolean conditionFlag;
            int oid,len;
            while (whereReadBuf.position()<wbound){
                conditionFlag = true;
                oid = whereReadBuf.getInt();
				int pos = (int)whereReadBuf.getLong();
				stringReadBuf.position(pos);
				len = stringReadBuf.getInt();
//                len = whereReadBuf.getInt();
                // if the length is not equal, skip the check statement, and set it as false
                if (len != conditionStr.length){
                    conditionFlag = false;
//                    whereReadBuf.position(whereReadBuf.position()+len); //skip valstr
                    continue;
                }
                valstr = new byte[len];
//                whereReadBuf.get(valstr);
				stringReadBuf.get(valstr);
                for(int i = 0; i < len; i++){
                    if(valstr[i] != conditionStr[i]){
                        // not equals
                        conditionFlag = false;
                        break;
                    }
                }
                if(conditionFlag){
                    // found it
                    //System.out.println("oid "+oid+ "results "+ new String(valstr));
                    if(oidList.contains(oid) == false)
						oidList.add(oid);
                }
                //System.out.println("oid "+oid+ "no found results "+ new String(valstr) + " "+new String(conditionStr));
            } // while whereReadBuf
        }// while true
        Collections.sort(oidList);
        resultSet = selectCondition(oidList,selectCols);*/
		return resultSet;
    }
    

    /* select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing 
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();    
        /*ByteBuffer whereBuf;
        List<Integer> oidList = new ArrayList<Integer>();
        String whereKey = new String(whereCol)+separator+"STRING";
        whereBuf = colBufs.get(whereKey);
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey); 
            return resultSet;
        }
        ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
        whereReadBuf.position(0);
		ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
		stringReadBuf.position(0);
        int wbound = whereBuf.position();        
        byte[] valstr;
        byte[] conditionStr = value;
        boolean conditionFlag;
        int oid,len;

		
        while (whereReadBuf.position()<wbound){
            conditionFlag = true;
            oid = whereReadBuf.getInt();
			int pos = (int)whereReadBuf.getLong();
			stringReadBuf.position(pos);
			len = stringReadBuf.getInt();
//            len = whereReadBuf.getInt();

			// if the length is not equal, skip the check statement, and set it as false
           	if (len != conditionStr.length){
            	conditionFlag = false;
  //          	whereReadBuf.position(whereReadBuf.position()+len); //skip valstr
            	continue;
            }

            valstr = new byte[len];
//            whereReadBuf.get(valstr);
			stringReadBuf.get(valstr);
            for(int i = 0; i < len; i++){
               if(valstr[i] != conditionStr[i]){
                    // not equals 
                    conditionFlag = false;
                    break;
                }   
            }
            if(conditionFlag){
                // found it 
                //System.out.println("oid "+oid+ "results "+ new String(valstr));
                oidList.add(oid);
                
            }
            //System.out.println("oid "+oid+ "no found results "+ new String(valstr) + " "+new String(conditionStr));
        } // while
		//for(int x:oidList)
		//	System.out.println(x); 
        resultSet = selectCondition(oidList,selectCols);

       */
		return resultSet;
    }

     /* Return the sum of specified columns if condition in where is met
    *  Assuming there's only one where column and there can be multiple select columns
    */
    public long selectWhere(byte[][] selectCols, byte[] whereCol, int threshold){    
        long sum = 0;
        long longnum = 0;
        //get where buf
        String whereKey = new String(whereCol) + separator + "LONG";
        ByteBuffer whereBuf = colBufs.get(whereKey);
        
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey); 
            return -1;
        }

        ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();       
        whereReadBuf.position(0);
        int wbound = whereBuf.position();        

        ByteBuffer [] selectBufs = new ByteBuffer [selectCols.length];
        int [] bounds = new int [selectCols.length];
        int [] positions = new int [selectCols.length];        

        int index = 0;
        for(String key: colBufs.keySet()){
            String columnKey = (key.split(separator))[0];
            for(int i = 0; i < selectCols.length; i++){
                if(Arrays.equals(columnKey.getBytes(), selectCols[i])==true){
                    //System.out.println("select buffer"+key);
                    ByteBuffer sbuf = colBufs.get(key);
                    //record current write position
                    bounds[index]=sbuf.position();
                    //create a read buffer for this column
                    ByteBuffer rBuf = sbuf.asReadOnlyBuffer();
                    rBuf.position(0);
                    positions[index] = 0;
                    selectBufs[index] = rBuf;
                    index += 1;
                }
            }
        }
        int rowSkipped = 0;

        while(whereReadBuf.position() < wbound){
            //boolean rowSelected = false;
            whereReadBuf.position((whereReadBuf.position() + 4)); //skip the object id
            longnum = whereReadBuf.getLong();
            
            if(longnum < threshold){
                for(int i = 0; i< index ; i++){
                    positions[i] += ((4+8)*rowSkipped + 4);
                    selectBufs[i].position(positions[i]);
                    longnum = selectBufs[i].getLong();
                    positions[i] += 8;
                    sum += longnum;
                }
                rowSkipped = 0;
            }
            else{
                //for(int i = 0; i< index ; i++){
                //    positions[i] = positions[i] + 4 + 8;
                //}
                rowSkipped++;
            }
        }
/*
        while(whereReadBuf.position() < wbound){
            boolean rowSelected = false;
            whereReadBuf.position((whereReadBuf.position() + 4)); //skip the object id
            longnum = whereReadBuf.getLong();
            if(longnum < threshold){
                rowSelected = true;
            }            

            for(int i = 0; i< index ; i++){
                if(rowSelected == true){
                    positions[i] += 4;
                    selectBufs[i].position(positions[i]);
                    longnum = selectBufs[i].getLong();
                    positions[i] += 8;
                    sum += longnum;
                }
                else{
                    positions[i] = positions[i] + 4 + 8;
                }
            }
        }*/
        return sum;
    }
    
    
    /* select count(*)  where a between value1 and value2 GROUP By K
     * range query, single column, long  type -- need to extend its type to include double
     */
    public Hashtable<String, Integer> aggregateRangeGroupBy(byte[][] selectCols, byte[] whereCol, long value1, long value2, byte[] gColumn)
    {
        
        Hashtable<String, Integer > resultSet= new Hashtable<String,Integer>();
        ByteBuffer whereBuf;
        List<Integer> oidList = new ArrayList<Integer>();
        String whereKey = new String(whereCol)+separator+"LONG";
        whereBuf = colBufs.get(whereKey);
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey);
            return resultSet;
        }
        ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
        whereReadBuf.position(0);
        int wbound = whereBuf.position();
        long value;
        boolean conditionFlag;
        int oid,len;
        while (whereReadBuf.position()<wbound){
            conditionFlag = false;
            oid = whereReadBuf.getInt();
            value = whereReadBuf.getLong();
            if( (value >= value1) && (value <= value2)){
                conditionFlag = true;
            }
            if(conditionFlag){
                // found it
                //System.out.println("found oid "+oid+ "results "+ value+"range "+value1+" "+value2);
                oidList.add(oid);
            }else{
                //System.out.println("no found oid "+oid+ "results "+ value+"range "+value1+" "+value2);
            }
            
        }//while
        
        // visit the group column and do the aggregation
        
        ByteBuffer groupBuf;
        // since we don't know type, we try STRING ,LONG,DOUBLE,BOOL -- could be dyn type
        String groupKey = new String(gColumn) + separator + "STRING";
        groupBuf=colBufs.get(groupKey);
        if(groupBuf != null){
            //found the right type
            System.out.println("aggregate group by string type clumn"+groupKey);
            aggregateByString(oidList, groupKey, groupBuf, resultSet);
            
        }
        
        groupKey = new String(gColumn) + separator + "LONG";
        //System.out.println("group by "+groupKey);
        groupBuf=colBufs.get(groupKey);
        if(groupBuf != null){
            System.out.println("aggregate group by long type column "+groupKey);
            //found the right type
            aggregateByLong(oidList,groupKey, groupBuf, resultSet);
            
        }
        groupKey = new String(gColumn) + separator + "DOUBLE";
        groupBuf=colBufs.get(groupKey);
        if(groupBuf != null){
            System.out.println("aggregate by double - to do");
            //found the right type
            //aggregateByDouble(oidList,groupKey, groupBuf, resultSet);
            
        }
        groupKey = new String(gColumn) + separator + "BOOL";
        groupBuf=colBufs.get(groupKey);
        if(groupBuf != null){
            System.out.println("aggregate by bool - to do");
            //found the right type
            //aggregateByBool(oidList,groupKey, groupBuf, resultSet);
            
        }
    
        
		return resultSet;

    }


    protected void aggregateByString(List<Integer> oidList, String key, ByteBuffer gBuf, Hashtable<String,Integer> resultSet){
        int bound = gBuf.position();
        ByteBuffer readBuf = gBuf.asReadOnlyBuffer();
        int oid=0;
        int len=0;
        byte[] valstr;
        int index = 0;
        int targetId = oidList.get(index);
        readBuf.position(0);
        if(resultSet == null){
            System.out.println("resultset is null");
            return;
        }
        while(readBuf.position()<bound){
            //check whether this oid is in the list
            oid=readBuf.getInt();
            if(oid == targetId){
                //found one, add it into the result set
                len=readBuf.getInt();
                valstr = new byte[len];
                readBuf.get(valstr);
                //aggregate the results
                String rKey = new String(valstr);
                Integer value = resultSet.get(rKey);
                if(value!=null){
                    resultSet.put(rKey, value+1);
                }else{
                    resultSet.put(rKey,1);
                }
                //update the targetId
                index ++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break; // we get results for all of oids
            }else{
                //skip this str
                len = readBuf.getInt();
                readBuf.position(readBuf.position()+ len);

            }// end if(oid == targetId)
        } //end while
       // System.out.println("Agg Resultset"+resultSet.size());
    }

    protected void aggregateByLong(List<Integer> oidList, String key, ByteBuffer gBuf, Hashtable<String,Integer> resultSet){
        int bound = gBuf.position();
        ByteBuffer readBuf = gBuf.asReadOnlyBuffer();
        int oid=0;
        int len=0;
        long longnum=0;
        int index = 0;
        int targetId = oidList.get(index);
        
        
        readBuf.position(0);
        if(resultSet == null){
            System.out.println("resultset is null");
            return;
        }
        while(readBuf.position()<bound){
            //check whether this oid is in the list
            oid=readBuf.getInt();
            if(oid == targetId){
                
                //found one, add it into the result set
                longnum=readBuf.getLong();
                //aggregate the results
                String rKey = new String(Long.toString(longnum));
                Integer value = resultSet.get(rKey);
                if(value!=null){
                    resultSet.put(rKey, value+1);
                }else{
                    resultSet.put(rKey,1);
                }
                //update the targetId
                index ++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break; // we get results for all of oids
            }else{
                //skip this str
                readBuf.position(readBuf.position()+ 8); //long is 8 bytes
                
            }// end if(oid == targetId)
        } //end while
       // System.out.println("Agg Resultset"+resultSet.size());
    }

    

    public long aggregateColumn(byte[] selectCol, byte[] whereCol, int threshold){
        long sum = 0;
        // for now, the where filed is LONG
        String whereKey = new String(whereCol) + separator + "LONG";
        ByteBuffer whereBuf = colBufs.get(whereKey);
        //System.out.println("search column key"+whereKey); 
        String selectKey = new String(selectCol) + separator + "LONG";
        ByteBuffer selectBuf = colBufs.get(selectKey);

        //ByteBuffer [] selectBufs = new ByteBuffer [colBufs.size()-1];
        int [] bounds = new int [colBufs.size() - 1];

        if(selectBuf == null){
            System.out.println("Didn't find buffer key"+selectKey); 
            return -1;
        }
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey); 
            return -1;
        }
        ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
        ByteBuffer selectReadBuf = selectBuf.asReadOnlyBuffer();

        int bound = whereBuf.position();
        int sbound = selectBuf.position();
        whereReadBuf.position(0);
        selectReadBuf.position(0); 
        int oid = 0;
        while(whereReadBuf.position()<bound){
            oid = whereReadBuf.getInt();
            long value = whereReadBuf.getLong();
            if(value <= threshold){
                int cOid = 0;
                while(selectReadBuf.position() < sbound){
                    cOid = selectReadBuf.getInt();
                    if(cOid > oid){
                        //position back
                        int newPosition = selectReadBuf.position()-Integer.SIZE/Byte.SIZE;
                        selectReadBuf.position(newPosition);
                        break;
                    }
                    else if(oid == cOid){
                        long sValue = selectReadBuf.getLong();
                        sum = sum + sValue;
                    }
                    else{
                        int newPosition = selectReadBuf.position()+Long.SIZE/Byte.SIZE;
                        selectReadBuf.position(newPosition);
                    }
                }
            }
        }
        
        return sum;
    }
    
    /**
     * execute a simple aggeration query with one condition check
     * first, just do scan, then do sum up
     * get the buffer for where column, and if selected, and get data from other buffers
     * for now, only aggregate LONG type
     */
    public long aggregate(byte[] colName, int threshold){
        long sum = 0;
        
        // for now, the where filed is LONG
        String whereKey = new String(colName) + separator + "LONG";
        ByteBuffer whereBuf = colBufs.get(whereKey);
        //System.out.println("search column key"+whereKey);
        
        ByteBuffer [] selectBufs = new ByteBuffer [colBufs.size()-1];
        int [] bounds = new int [colBufs.size() - 1];
        
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey);
            return -1;
        }
        int index = 0;
        for(String key: colBufs.keySet()){
            if( key.equals(whereKey)!=true ) {
                //System.out.println("select buffer"+key);
                ByteBuffer sbuf = colBufs.get(key);
                //record current write position
                bounds[index]=sbuf.position();
                //create a read buffer for this column
                ByteBuffer rBuf = sbuf.asReadOnlyBuffer();
                rBuf.position(0);
                selectBufs[index] = rBuf;
                index += 1;
            }else{
                //System.out.println("where buffer"+key);
            }
        }
        
        ByteBuffer readBuf = whereBuf.asReadOnlyBuffer();
        int bound = whereBuf.position();
        readBuf.position(0);
        //have a loop to scan each record in readBuf field
        // check selectivity first
        int oid = 0;
        while(readBuf.position()<bound){
            oid = readBuf.getInt();
            long value = readBuf.getLong();
            if(value <= threshold){
                //get other columns and sum up
                for(int i = 0; i< index ; i++){
                    //similar as getObjectFromLongBuf
                    ByteBuffer rBuf = selectBufs[i];
                    int wBound = bounds[i];
                    int cOid = 0;
                    while(rBuf.position() < bound){
                        //read current object id
                        cOid = rBuf.getInt();
                        if(cOid > oid){
                            //position back
                            int newPosition = rBuf.position()-Integer.SIZE/Byte.SIZE;
                            rBuf.position(newPosition);
                            break;
                        }
                        //if(cOid == oid){
                        //System.out.print("Row "+cOid);
                        //}
                        //read Long
                        if(cOid == oid){
                            long sValue = rBuf.getLong();
                            sum = sum + sValue;
                        }else{
                            //skip long
                            int newPosition = rBuf.position()+Long.SIZE/Byte.SIZE;
                            rBuf.position(newPosition);
                        }
                    } // while rBuf
                } //for index
            }//threshold
        }
        
        return sum;
        
    }

    public void printBriefStats(){
        result.printBriefStats();
        return;
    }

    public static void main(String[] args) throws IOException{


        /* flatten the json file */ 
        //JsonReader reader = Json.createReader(new FileReader("testjson/test.json")); 
        JsonReader reader = Json.createReader(new FileReader("testjson/abc.json")); 
        JsonObject jsonob = reader.readObject();
        System.out.println(jsonob.toString());
        ColStoreEng store= new ColStoreEng(10*1000*1000);
        int objid = 1;
        store.insertObject(objid,jsonob,null);
        store.insertObject(2,jsonob,null);
        store.insertObject(3,jsonob,null);
        /* populate the table */

        System.out.println("get the result out \n");
        /* objid, keystr,valstr,valnum,valbool - 5 bytes */
        /* read it out */
        store.getObject(2);

        //aggregate scan
        String targetColumn = "A";
        long sum = store.aggregate(targetColumn.getBytes(),10);
        System.out.println("Aggregate sum results :"+sum);

    }
}
