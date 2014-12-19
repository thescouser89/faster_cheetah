
/**
 * RowColStore
 *
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
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

public class RowColStoreEng extends StoreEngine {
	private ByteBuffer stringBuffer;
	//private static String [][] fields;
    /* key as the key for the hash table */
    Hashtable<String, String> hybridBufs; /* multiple keys could direct to the same buffer */
    Hashtable<String, ByteBuffer> searchBufs; /* one key one buffer - for get the object out */
	Hashtable<String, Long> tempBufLong;
	Hashtable<String, Double> tempBufDouble;
	Hashtable<String, String> tempBufString;
	Hashtable<String, String> tempBufBool;
    private int numOfNull;
    private int numOfDataPt;
    private int numOfRowId;
    
    ResultSet result;

	/**
         * Creates a ring buffer, and using Column format to store the data
	 * initialize the buffer size 
	 */
	public RowColStoreEng (int memory_size_in_bytes,String layoutFile)
	{
		super(memory_size_in_bytes);
        stringBuffer = ByteBuffer.allocateDirect(max_buf_size);

		hybridBufs = new Hashtable<String, String>();
        searchBufs = new Hashtable<String, ByteBuffer>();
        tempBufLong = new Hashtable<String, Long>();
        tempBufDouble = new Hashtable<String, Double>();
        tempBufString = new Hashtable<String, String>();
		tempBufBool = new Hashtable<String, String>();
        numOfDataPt = 0;
        numOfNull = 0;
        numOfRowId = 0;
        
        result = new ResultSet(100*1000*1000);
        
        /* create buffer first */
        /* read layout file, create each buffer for each line */
        /* key is field_name:type, value is the buf */

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(layoutFile));
            String line  = bufferedReader.readLine();
            //int i = 0;
            while(line !=null){
                String[] keys = line.split("\\s+"); //separted by any white space 
                if(keys.length == 0)  
                    continue; // empty line

                for (String k : keys){
					//k is in the form of "field_name:TYPE" ex: "num:LONG"
                    hybridBufs.put(k,line);
                }
				ByteBuffer buf;// = ByteBuffer.allocateDirect(max_buf_size);
				if((keys.length==1)&&(line.matches("sparse_"))){
					buf = ByteBuffer.allocateDirect(2000);
				}else{
					buf = ByteBuffer.allocateDirect(max_buf_size/1000*keys.length);
				}
                searchBufs.put(line,buf);
              //  i++;
                line = bufferedReader.readLine();
            }
        } catch (FileNotFoundException e){
			System.err.println("FileNotFoundException:"+e.getMessage());
			return ;
        } catch (IOException e){
			System.err.println("IOException:"+e.getMessage());
			return;
        }
	}

    public double getRowIdRatio(){
        return (double)numOfRowId / (double)numOfDataPt;
    }
    
    public double getNullRatio(){
        return (double)numOfNull ;
    }
    /*
    public HashMap<Integer, HashMap<String, String>> selectold(byte[][] columns){
        HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
		HashMap<String, ByteBuffer> bufKeys = new HashMap<String, ByteBuffer>();
		for(int i = 0; i < columns.length; i++){
            String[] types = new String[]{"STRING", "LONG", "DOUBLE", "BOOL"};
            for(int j = 0; j < types.length; j++){
                String colKey = new String(columns[i]) + separator + types[j];
                String bufKey = hybridBufs.get(colKey);
                if(bufKey != null){
                    ByteBuffer buf = searchBufs.get(bufKey);
                    if(buf != null){
                        bufKeys.put(bufKey, buf);
                    }
                }
            }
		}

		for(String skey: bufKeys.keySet()){
		    String[] keys = skey.split("\\s+"); //separted by any white space
			String[] types = new String[keys.length];
        	int[] selectFields = new int[keys.length];

			for(int i = 0; i < keys.length; i++){
				selectFields[i] = 0;
				//keys are in the form of "field_name:TYPE" ex: "num:LONG"
				String[] parts = keys[i].split(separator);
				String columnKey = parts[0];
				types[i] = parts[1];

				for(int j = 0; j < columns.length; j++){
	 				if(Arrays.equals(columnKey.getBytes(), columns[j])==true){
						selectFields[i] = 1;
					}
				}
			}
            ByteBuffer buf = bufKeys.get(skey);
            ByteBuffer readBuf = buf.asReadOnlyBuffer();
            readBuf.position(0);
            long longnum = 0;
            double num = 0;
            int oid=0, len=0, position = 0, bound = buf.position();
            byte[] valstr,valbool;

			HashMap <String, String> innerSet;
         	while(readBuf.position()<bound){
                oid=readBuf.getInt();
                position += 4;
				innerSet = resultSet.get(oid);
				if(innerSet == null){
					innerSet = new HashMap<String, String>();
				}
				for(int i = 0 ; i < keys.length; i++){
                    if(types[i].equals("STRING")){
                        readBuf.position(position);
                        len=readBuf.getInt();
                        position += 4;
                        
                        if(len != UNDEFINED){
                            if(selectFields[i] == 1){
                                valstr = new byte[len];
                                readBuf.get(valstr);
                                innerSet.put(keys[i], new String(valstr));
                            }
                            position += len;
                        }
                    } else if(types[i].equals("LONG")){
                        if(selectFields[i] == 1){
                            readBuf.position(position);
                            longnum = readBuf.getLong();
                            if(longnum != UNDEFINED){
                                innerSet.put(keys[i],  String.valueOf(longnum));
							}
                        }
                        position += 8;
                    } else if(types[i].equals("DOUBLE")){
                        if(selectFields[i] == 1){
                            readBuf.position(position);
                            num = readBuf.getDouble();
                            if(longnum != UNDEFINED){
								innerSet.put(keys[i], String.valueOf(num));
							}
                        }
                        position += 8;
                    } else if(types[i].equals("BOOL")){
                   		if(selectFields[i] == 1){
							valbool = new byte[1];
                            readBuf.position(position);
                            readBuf.get(valbool);
                            if(valbool[0] != UndefByte[0]){
   								innerSet.put(keys[i], String.valueOf(valbool[0]));
							}
                        }
                        position += 1;
					}
				}
                readBuf.position(position);
				if(!innerSet.isEmpty()){
					resultSet.put(oid, innerSet);
				}
			}	
		}
		return resultSet;
	}
    */
    
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
            //len=readBuf.getInt();
            int pos = (int)readBuf.getLong();
            //stringReadBuf.position(pos);
            //len = stringReadBuf.getInt();
            //valstr = new byte[len];
            //stringReadBuf.get(valstr);
            result.addString(selectKey, oid, pos);
            //innerResults = resultSet.get(oid);
            //if(innerResults == null){
            //    innerResults = new HashMap<String, String>();
            //}
            //innerResults.put(selectKey, new String(valstr));
            //resultSet.put(oid, innerResults);
        }
        //return resultSet;
        return;
	}
    
    protected void selectLong(String selectKey, ByteBuffer selectBuf){
        
        //HashMap <String, String> innerResults;
        int bound = selectBuf.position();
        long longnum = 0;
        int oid = 0;
        ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
        readBuf.position(0);
        while(readBuf.position()<bound){
            oid=readBuf.getInt();
            longnum = readBuf.getLong();
            result.addLong(selectKey, oid, longnum);

            /*innerResults = resultSet.get(oid);
            if(innerResults == null){
                innerResults = new HashMap<String, String>();
            }
            innerResults.put(selectKey, String.valueOf(longnum));
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
    
    public void select(byte[][] columns){
       //HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
        
        result.clearResultSet();
        
		HashMap<String, ByteBuffer> bufKeys = new HashMap<String, ByteBuffer>();
        
		for(int i = 0; i < columns.length; i++){
            String[] types = new String[]{"STRING", "LONG", "DOUBLE", "BOOL"};
            for(int j = 0; j < types.length; j++){
                String colKey = new String(columns[i]) + separator + types[j];
                String bufKey = hybridBufs.get(colKey);
                if(bufKey != null){
                    ByteBuffer buf = searchBufs.get(bufKey);
                    if(buf != null){
                        bufKeys.put(bufKey, buf);
                    }
                }
            }
		}

		for(String skey: bufKeys.keySet()){
            String[] keyType = skey.split("\\s+"); //separted by any white space
            
            //this partition is column based
            if(keyType.length == 1){
                String[] parts = keyType[0].split(separator);
				String columnKey = parts[0];
				String type = parts[1];
                
                if(type.equals("STRING"))
                    selectString(columnKey, bufKeys.get(skey));
                    //resultSet = selectString(columnKey, bufKeys.get(skey), resultSet);
                else if(type.equals("LONG"))
                    selectLong(columnKey, bufKeys.get(skey));
                    //resultSet = selectLong(columnKey, bufKeys.get(skey), resultSet);
                else if(type.equals("DOUBLE"))
                    selectDouble(columnKey, bufKeys.get(skey));
                    //resultSet = selectDouble(columnKey, bufKeys.get(skey), resultSet);
                else if(type.equals("BOOL"))
                    selectBool(columnKey, bufKeys.get(skey));
                    //resultSet = selectBool(columnKey, bufKeys.get(skey), resultSet);

                continue;
            }
            
            String[] types = new String[keyType.length];
            int[] skip = new int[keyType.length];
            int[] columnIndexes = new int[keyType.length];
            int count = 0, index = 0;
            
            for(int i = 0; i < keyType.length; i++){
                //keyType are in the form of "field_name:TYPE" ex: "num:LONG"
				String[] parts = keyType[i].split(separator);
				String columnKey = parts[0];
				types[i] = parts[1];
                
                boolean fieldSelected = false;
                for(int j = 0; j < columns.length; j++){
                    if(Arrays.equals(columnKey.getBytes(), columns[j])==true){
                        fieldSelected = true;
                        columnIndexes[index] = i;
                        skip[index] = count;
                        index++;
                        count = 0;
                    }
                }
                if(fieldSelected == false){
                    if(types[i].equals("BOOL")){
                        count += 1;
                    }else{
                        count += 8;
                    }
                }
            }
            int skipToRowEnd = count;
            
            ByteBuffer buf = bufKeys.get(skey);
            ByteBuffer readBuf = buf.asReadOnlyBuffer(), stringReadBuf = stringBuffer.asReadOnlyBuffer();
            readBuf.position(0);
            stringReadBuf.position(0);
            long longnum = 0;
            double num = 0;
            int oid=0, len=0, bound = buf.position(), newpos;
            byte[] valstr,valbool;
			//HashMap <String, String> innerSet;
            
            while(readBuf.position()<bound){
                oid=readBuf.getInt();
				//innerSet = resultSet.get(oid);
				//if(innerSet == null){
				//	innerSet = new HashMap<String, String>();
				//}
                for(int i = 0 ; i < index; i++){
                    if(skip[i] != 0){
                        newpos = readBuf.position() + skip[i];
                        readBuf.position(newpos);
                    }
                    int columnIndex = columnIndexes[i];
                    if(types[columnIndex].equals("STRING")){
                        int pos = (int)readBuf.getLong();
                        if(pos == UNDEFINED){
                            continue;
                        }
                        //stringReadBuf.position(pos);
                        //len = stringReadBuf.getInt();
                        //valstr = new byte[len];
                        //stringReadBuf.get(valstr);
                        result.addString(keyType[columnIndex], oid, pos);
                        //resultSet.put(oid, keyType[columnIndex] + ":" + new String(valstr));
                            //innerSet.put(keyType[columnIndex], new String(valstr));
                        //}
                    }else if(types[columnIndex].equals("LONG")){
                        longnum = readBuf.getLong();
                        if(longnum != UNDEFINED)
                            result.addLong(keyType[columnIndex], oid, longnum);
                            //resultSet.put(oid, keyType[columnIndex] + ":" + String.valueOf(longnum));
                            //innerSet.put(keyType[columnIndex],  String.valueOf(longnum));
                    }else if(types[columnIndex].equals("DOUBLE")){
                        num = readBuf.getDouble();
                        if(num != UNDEFINED)
                            result.addDouble(keyType[columnIndex], oid, num);
                            //resultSet.put(oid, keyType[columnIndex] + ":" + String.valueOf(num));
//                            innerSet.put(keyType[columnIndex], String.valueOf(num));
                    }else if(types[columnIndex].equals("BOOL")){
                        valbool = new byte[1];
                        readBuf.get(valbool);
                        if(valbool[0] != UndefByte[0])
                            result.addBool(keyType[columnIndex], oid, valbool[0]);
                            //resultSet.put(oid, keyType[columnIndex] + ":" + String.valueOf(valbool[0]));
//                            innerSet.put(keyType[columnIndex], String.valueOf(valbool[0]));
                    }
                }
                if(skipToRowEnd != 0){
                    newpos = readBuf.position() + skipToRowEnd;
                    readBuf.position(newpos);
                }
                //if(!innerSet.isEmpty()){
                //    resultSet.put(oid, innerSet);
                //}
            }
        }
        //return resultSet;
        return;
	}

    public void selectFromBuf(byte[][] selectCols, ByteBuffer buffer, String bufKey, List<Integer> oidList){
        String[] keyType = bufKey.split("\\s+"); //separted by any white space
        String[] types = new String[keyType.length];
        int[] skip = new int[keyType.length], columnIndexes = new int[keyType.length];
        int count = 0, index = 0, rowLength = 0;
        
        for(int i = 0; i < keyType.length; i++){
            //keyType are in the form of "field_name:TYPE" ex: "num:LONG"
            String[] parts = keyType[i].split(separator);
            String columnKey = parts[0];
            types[i] = parts[1];
            
            if(types[i].equals("BOOL"))
                rowLength += 1;
            else
                rowLength += 8;
            
            if(selectCols[0][0]==(byte) '*'){
                columnIndexes[index] = i;
                skip[index] = count;
                index++;
                continue;
            }

            boolean fieldSelected = false;
            for(int j = 0; j < selectCols.length; j++){
                if(Arrays.equals(columnKey.getBytes(), selectCols[j])==true){
                    fieldSelected = true;
                    columnIndexes[index] = i;
                    skip[index] = count;
                    index++;
                    count = 0;
                }
            }
            if(fieldSelected == false){
                if(types[i].equals("BOOL")){
                    count += 1;
                }else{
                    count += 8;
                }
            }
        }
        ByteBuffer readBuf = buffer.asReadOnlyBuffer(), stringReadBuf = stringBuffer.asReadOnlyBuffer();
        readBuf.position(0);
        long longnum = 0;
        double num = 0;
        int oid=0, len=0, bound = buffer.position(), newpos, x = 0, targetId = oidList.get(x),rowStartPosition = 0, skipToRowEnd = count;
        byte[] valstr,valbool;
        //HashMap <String, String> innerSet;

        while(readBuf.position()<bound){
            oid=readBuf.getInt();
            rowStartPosition = readBuf.position();
            //check whether this oid is in the list
            if(oid == targetId){
                //found one, add it into the result set
                /*innerSet = resultSet.get(oid);
                if(innerSet == null){
                    innerSet = new HashMap<String, String>();
                }*/
                
                for(int i = 0 ; i < index; i++){
                    if(skip[i] != 0){
                        newpos = readBuf.position() + skip[i];
                        readBuf.position(newpos);
                    }
                    int columnIndex = columnIndexes[i];
                    if(types[columnIndex].equals("STRING")){
                        int pos = (int)readBuf.getLong();
                        if(pos == UNDEFINED){
                            continue;
                        }
                        result.addString(keyType[columnIndex], oid, pos);
                        
                        /*stringReadBuf.position(pos);
                        len = stringReadBuf.getInt();
                        valstr = new byte[len];
                        stringReadBuf.get(valstr);
                        innerSet.put(keyType[columnIndex], new String(valstr));*/
                    }else if(types[columnIndex].equals("LONG")){
                        longnum = readBuf.getLong();
                        if(longnum != UNDEFINED)
                            result.addLong(keyType[columnIndex], oid, longnum);
                            //innerSet.put(keyType[columnIndex],  String.valueOf(longnum));
                    }else if(types[columnIndex].equals("DOUBLE")){
                        num = readBuf.getDouble();
                        if(num != UNDEFINED)
                            result.addDouble(keyType[columnIndex], oid, num);
                            //innerSet.put(keyType[columnIndex], String.valueOf(num));
                    }else if(types[columnIndex].equals("BOOL")){
                        valbool = new byte[1];
                        readBuf.get(valbool);
                        if(valbool[0] != UndefByte[0])
                            result.addBool(keyType[columnIndex], oid, valbool[0]);

//                            innerSet.put(keyType[columnIndex], String.valueOf(valbool[0]));
                    }
                }
                if(skipToRowEnd != 0){
                    newpos = readBuf.position() + skipToRowEnd;
                    readBuf.position(newpos);
                }
                //if(!innerSet.isEmpty()){
                //    resultSet.put(oid, innerSet);
                //}
                //update the targetId
                x ++;
                if(x < oidList.size())
                    targetId = oidList.get(x);
                else
                    break; // we get results for all of oids
            } else if(oid > targetId){
                x ++;
                if(x < oidList.size())
                    targetId = oidList.get(x);
                else
                    break;
                readBuf.position(rowStartPosition - 4);
            } else{
                //skip this str
                len = readBuf.getInt();
                readBuf.position(rowStartPosition + rowLength);
            }
        }
//        return resultSet;
        return;
    }
    
    /* select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] conditionValue){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
		/*ByteBuffer whereBuf;
        List<Integer> oidList = new ArrayList<Integer>();
        String whereKey = new String(whereCol)+separator+"STRING";
		//String colKey = new String(whereCol) + separator + "STRING";
        String bufKey = hybridBufs.get(whereKey);
        whereBuf = searchBufs.get(bufKey);

		if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey);
            return resultSet;
        }
        
        ByteBuffer stringReadBuf = stringBuffer.asReadOnlyBuffer();
		if(whereKey.equals(bufKey)){
		//This is a col-based for sure
			ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
        	whereReadBuf.position(0);
        	int wbound = whereBuf.position();
       		byte[] valstr;
        	byte[] conditionStr = conditionValue;
        	boolean conditionFlag;
        	int oid,len;
        	while (whereReadBuf.position()<wbound){
            	conditionFlag = true;
            	oid = whereReadBuf.getInt();
				int pos = (int)whereReadBuf.getLong();
				stringReadBuf.position(pos);
				len = stringReadBuf.getInt();
				// if the length is not equal, skip the check statement, and set it as false
           		if (len != conditionStr.length){
            		conditionFlag = false;
            		continue;
            	}
            	valstr = new byte[len];
				stringReadBuf.get(valstr);
            	for(int i = 0; i < len; i++){
                	if(valstr[i] != conditionStr[i]){
                	    conditionFlag = false;
                	    break;
              		}
            	}
            	if(conditionFlag){
                	oidList.add(oid);
            	}
        	} // while
        	resultSet = selectCondition(oidList,selectCols, resultSet, "");
		}else{
			String[] keyType = bufKey.split("\\s+"); //separted by any white space
            String[] types = new String[keyType.length];
            int[] skip = new int[keyType.length], columnIndexes = new int[keyType.length], whereSkip = new int[keyType.length], whereIndexes = new int[keyType.length];
            int count = 0, count2 = 0, index = 0, index2 = 0;
            
            for(int i = 0; i < keyType.length; i++){
                //keyType are in the form of "field_name:TYPE" ex: "num:LONG"
				String[] parts = keyType[i].split(separator);
				String columnKey = parts[0];
				types[i] = parts[1];
                boolean isSelectField = false, isWhereField = false;
                
                if(selectCols[0][0]==(byte) '*'){
                    columnIndexes[index] = i;
                    skip[index] = count;
                    index++;
                    isSelectField = true;
                    count = 0;
                }else{
                    for(int j = 0; j < selectCols.length; j++){
                        if(Arrays.equals(columnKey.getBytes(), selectCols[j])==true){
                            columnIndexes[index] = i;
                            skip[index] = count;
                            index++;
                            count = 0;
                            isSelectField = true;
                        }
                    }
                }
                if((Arrays.equals(columnKey.getBytes(), whereCol)==true) &&(types[i].equals("STRING"))){
                    whereIndexes[index2] = i;
                    whereSkip[index2] = count2;
                    index2++;
                    count2 = 0;
                    isWhereField = true;
                }
                if(isWhereField == false){
                    if(types[i].equals("BOOL"))
                        count2 += 1;
                    else
                        count2 += 8;
                }
                if(isSelectField == false){
                    if(types[i].equals("BOOL"))
                        count += 1;
                    else
                        count += 8;
                }
            }
            int skipToRowEnd = count, lastWhereToEnd = count2, bound = whereBuf.position(), oid=0, len = 0, newpos, rowStartPosition = 0;
            ByteBuffer readBuf = whereBuf.asReadOnlyBuffer();
            long longnum = 0, value;
            double num = 0;
            byte[] valstr, valbool, conditionStr = conditionValue;
            boolean conditionFlag;
            readBuf.position(0);
            
            while(readBuf.position()<bound){
                oid=readBuf.getInt();
                HashMap <String, String> innerSet = new HashMap<String, String>();
                rowStartPosition = readBuf.position();
                conditionFlag = false;
                //skip to where col first
                for(int i = 0 ; i < index2; i++){
                    if(whereSkip[i] != 0){
                        newpos = readBuf.position() + whereSkip[i];
                        readBuf.position(newpos);
                    }
                    int pos = (int)readBuf.getLong();
                    if((pos == UNDEFINED) || (conditionFlag == true)){
                        continue;
                    }
                    stringReadBuf.position(pos);
                    len = stringReadBuf.getInt();
                    conditionFlag = true;
                    // if the length is not equal, skip the check statement, and set it as false
                    if (len != conditionStr.length){
                         conditionFlag = false;
                         continue;
                    }
                    valstr = new byte[len];
                    stringReadBuf.get(valstr);
                    for(int j = 0; j < len; j++){
                         if(valstr[j] != conditionStr[j]){
                               conditionFlag = false;
                               break;
                         }
                    }
                }
                if(conditionFlag){
                    oidList.add(oid);
                    readBuf.position(rowStartPosition);
                    
                    for(int i = 0 ; i < index; i++){
                        if(skip[i] != 0){
                            newpos = readBuf.position() + skip[i];
                            readBuf.position(newpos);
                        }
                        int columnIndex = columnIndexes[i];
                        if(types[columnIndex].equals("STRING")){
                            int pos = (int)readBuf.getLong();
                            if(pos == UNDEFINED){
                                continue;
                            }
                            stringReadBuf.position(pos);
                            len = stringReadBuf.getInt();
                            valstr = new byte[len];
                            stringReadBuf.get(valstr);
                            innerSet.put(keyType[columnIndex], new String(valstr));
                        }else if(types[columnIndex].equals("LONG")){
                            longnum = readBuf.getLong();
                            if(longnum != UNDEFINED)
                                innerSet.put(keyType[columnIndex],  String.valueOf(longnum));
                        }else if(types[columnIndex].equals("DOUBLE")){
                            num = readBuf.getDouble();
                            if(num != UNDEFINED)
                                innerSet.put(keyType[columnIndex], String.valueOf(num));
                        }else if(types[columnIndex].equals("BOOL")){
                            valbool = new byte[1];
                            readBuf.get(valbool);
                            if(valbool[0] != UndefByte[0])
                                innerSet.put(keyType[columnIndex], String.valueOf(valbool[0]));
                        }
                    }
                    if(skipToRowEnd != 0){
                        newpos = readBuf.position() + skipToRowEnd;
                        readBuf.position(newpos);
                    }
                }else{
                    //skip to the end of row
                    if(lastWhereToEnd != 0){
                        newpos = readBuf.position() + lastWhereToEnd;
                        readBuf.position(newpos);
                    }
                }
                if(!innerSet.isEmpty()){
                    resultSet.put(oid, innerSet);
                }
			}//end of while
            resultSet = selectCondition(oidList,selectCols, resultSet, bufKey);
        }//end of else*/
        return resultSet;
    }
    
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2){
        
        result.clearResultSet();
        
       // HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
		ByteBuffer whereBuf;
        List<Integer> oidList = new ArrayList<Integer>();
        String whereKey = new String(whereCol)+separator+"LONG";
		//String colKey = new String(whereCol) + separator + "STRING";
        String bufKey = hybridBufs.get(whereKey);
        whereBuf = searchBufs.get(bufKey);
        
		if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey);
            //return resultSet;
            return;
        }
        
		if(whereKey.equals(bufKey)){
            //This is a col-based for sure
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
                    oidList.add(oid);
                }
            }//while
            //resultSet = selectCondition(oidList,selectCols,resultSet,"");
            selectCondition(oidList,selectCols,"");
		}else{
			String[] keyType = bufKey.split("\\s+"); //separted by any white space
            String[] types = new String[keyType.length];
            int[] skip = new int[keyType.length], columnIndexes = new int[keyType.length], whereSkip = new int[keyType.length], whereIndexes = new int[keyType.length];
            int count = 0, count2 = 0, index = 0, index2 = 0;
            
            for(int i = 0; i < keyType.length; i++){
                //keyType are in the form of "field_name:TYPE" ex: "num:LONG"
				String[] parts = keyType[i].split(separator);
				String columnKey = parts[0];
				types[i] = parts[1];
                boolean isSelectField = false, isWhereField = false;
                
                if(selectCols[0][0]==(byte) '*'){
                    columnIndexes[index] = i;
                    skip[index] = count;
                    index++;
                    isSelectField = true;
                    count = 0;
                }else{
                    for(int j = 0; j < selectCols.length; j++){
                        if(Arrays.equals(columnKey.getBytes(), selectCols[j])==true){
                            columnIndexes[index] = i;
                            skip[index] = count;
                            index++;
                            count = 0;
                            isSelectField = true;
                        }
                    }
                }
                if((Arrays.equals(columnKey.getBytes(), whereCol)==true) && (types[i].equals("LONG"))){
                    whereIndexes[index2] = i;
                    whereSkip[index2] = count2;
                    index2++;
                    count2 = 0;
                    isWhereField = true;
                }
                if(isWhereField == false){
                    if(types[i].equals("BOOL"))
                        count2 += 1;
                    else
                        count2 += 8;
                }
                if(isSelectField == false){
                    if(types[i].equals("BOOL"))
                        count += 1;
                    else
                        count += 8;
                }
            }
            int skipToRowEnd = count, lastWhereToEnd = count2, bound = whereBuf.position(), oid=0, len = 0, newpos, rowStartPosition = 0;
            ByteBuffer readBuf = whereBuf.asReadOnlyBuffer(), stringReadBuf = stringBuffer.asReadOnlyBuffer();
            long longnum = 0, value;
            double num = 0;
            byte[] valstr, valbool;
            boolean conditionFlag;
            readBuf.position(0);
            
            while(readBuf.position()<bound){
                oid=readBuf.getInt();
                //HashMap <String, String> innerSet = new HashMap<String, String>();
                rowStartPosition = readBuf.position();
                conditionFlag = false;
                //skip to where col first
                for(int i = 0 ; i < index2; i++){
                    if(whereSkip[i] != 0){
                        newpos = readBuf.position() + whereSkip[i];
                        readBuf.position(newpos);
                    }
                    value = readBuf.getLong();
                    if(value != UNDEFINED){
                        if( (value >= value1) && (value <= value2)){
                            conditionFlag = true;
                        }
                    }
                }
                if(conditionFlag){
                    oidList.add(oid);
                    readBuf.position(rowStartPosition);
                    for(int i = 0 ; i < index; i++){
                        if(skip[i] != 0){
                            newpos = readBuf.position() + skip[i];
                            readBuf.position(newpos);
                        }
                        int columnIndex = columnIndexes[i];
                        if(types[columnIndex].equals("STRING")){
                            int pos = (int)readBuf.getLong();
                            if(pos == UNDEFINED){
                                continue;
                            }
                            result.addString(keyType[columnIndex], oid, pos);
                            //stringReadBuf.position(pos);
                            //len = stringReadBuf.getInt();
                            //valstr = new byte[len];
                            //stringReadBuf.get(valstr);
                            //innerSet.put(keyType[columnIndex], new String(valstr));
                        }else if(types[columnIndex].equals("LONG")){
                            longnum = readBuf.getLong();
                            if(longnum != UNDEFINED)
                                result.addLong(keyType[columnIndex], oid, longnum);
                                //innerSet.put(keyType[columnIndex],  String.valueOf(longnum));
                        }else if(types[columnIndex].equals("DOUBLE")){
                            num = readBuf.getDouble();
                            if(num != UNDEFINED)
                                result.addDouble(keyType[columnIndex], oid, num);
                                //innerSet.put(keyType[columnIndex], String.valueOf(num));
                        }else if(types[columnIndex].equals("BOOL")){
                            valbool = new byte[1];
                            readBuf.get(valbool);
                            if(valbool[0] != UndefByte[0])
                                result.addBool(keyType[columnIndex], oid, valbool[0]);
                                //innerSet.put(keyType[columnIndex], String.valueOf(valbool[0]));
                        }
                    }
                    if(skipToRowEnd != 0){
                        newpos = readBuf.position() + skipToRowEnd;
                        readBuf.position(newpos);
                    }
                }else{
                    //skip to the end of row
                    if(lastWhereToEnd != 0){
                        newpos = readBuf.position() + lastWhereToEnd;
                        readBuf.position(newpos);
                    }
                }
                //if(!innerSet.isEmpty()){
                //    resultSet.put(oid, innerSet);
                //}
			}//end of while
            //resultSet = selectCondition(oidList,selectCols, resultSet, bufKey);
            selectCondition(oidList,selectCols, bufKey);
        }//end of else
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
    	//HashMap<Integer, HashMap<String, String>> resultSet = selectWhere(selectCols, whereCol, "selectWhereAny", 0, 0, relation, value);
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
    	return resultSet;
	}
    
    
     
    /*
     *    Given a list of oids, select the corresponding columns which match the oids and put them into results
     *    Return a result set
     *    Called by select where executor
     */
    
    protected void selectCondition(List<Integer> oidList, byte[][] columns, String bufToSkip){
        //HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
        /* first handle select all case - later we need to optimize it to get values for each column  */
        if (oidList.size() == 0)
            return;
            //return resultSet;
        
        Hashtable<String, ByteBuffer> selectBufs = new Hashtable<String, ByteBuffer>();
        if(columns[0][0]==(byte) '*' ){
            //System.out.println("select all where");
            //tranverse all of columns
            for(String selectKey: hybridBufs.keySet()){
                // check the key and assign different method
                //ByteBuffer selectBuf = searchBufs.get(selectKey);
                String bufKey = hybridBufs.get(selectKey);
                ByteBuffer selectBuf = searchBufs.get(bufKey);

                if(!(bufKey.equals(selectKey))){//this is NOT a col-based partition
                    selectBufs.put(bufKey, selectBuf);
                    continue;
                }
                
                String [] parts = selectKey.split(separator);
                String columnKey = parts[0];
                String type = parts[1];
                
                if(type.equals("STRING"))
                    selectConditionString(oidList, selectKey, selectBuf);
                    //resultSet = selectConditionString(oidList, selectKey, selectBuf, resultSet);
                else if(type.equals("LONG"))
                    selectConditionLong(oidList,selectKey, selectBuf);
                    //resultSet = selectConditionLong(oidList,selectKey, selectBuf, resultSet);
                else if(type.equals("DOUBLE"))
                    selectConditionDouble(oidList,selectKey, selectBuf);
                    //resultSet = selectConditionDouble(oidList,selectKey, selectBuf, resultSet);
                else if(type.equals("BOOL"))
                    selectConditionBool(oidList,selectKey, selectBuf);
                    //resultSet = selectConditionBool(oidList,selectKey, selectBuf, resultSet);
                else{
                    System.out.println("Error: no such type in buf "+type);
                    break;
                }
            } //end for
            for(String skey: selectBufs.keySet()){
                if(skey.equals(bufToSkip)){
                    continue;
                }
                ByteBuffer buf = selectBufs.get(skey);
                selectFromBuf(columns, buf, skey, oidList);
                //resultSet = selectFromBuf(columns, resultSet, buf, skey, oidList);
            }
            return;
            //return resultSet;
        }
        // tranverse colBufs,find the proper column buf
        for(int i = 0; i< columns.length; i++){
            ByteBuffer selectBuf;
            // since we don't know type, we try STRING ,LONG,DOUBLE,BOOL -- could be dyn type
			String selectKey = new String(columns[i]) + separator + "STRING";
            String bufKey = hybridBufs.get(selectKey);
            //System.out.println(bufKey + ":" + selectKey);
			if(bufKey != null){
                selectBuf=searchBufs.get(bufKey);
                //found the right type
                if(bufKey.equals(selectKey)){
                    selectConditionString(oidList, selectKey, selectBuf);
                    //resultSet = selectConditionString(oidList, selectKey, selectBuf, resultSet);
                }else{
                    selectBufs.put(bufKey, selectBuf);
                }
            }
			selectKey = new String(columns[i]) + separator + "LONG";
            bufKey = hybridBufs.get(selectKey);
			if(bufKey != null){
                selectBuf=searchBufs.get(bufKey);
                //found the right type
                if(bufKey.equals(selectKey)){
                    selectConditionLong(oidList,selectKey, selectBuf);
                    //resultSet = selectConditionLong(oidList,selectKey, selectBuf, resultSet);
                }else{
                    selectBufs.put(bufKey, selectBuf);
                }
            }
			selectKey = new String(columns[i]) + separator + "DOUBLE";
            bufKey = hybridBufs.get(selectKey);
			if(bufKey != null){
                selectBuf=searchBufs.get(bufKey);
                //found the right type
                if(bufKey.equals(selectKey)){
                    selectConditionDouble(oidList,selectKey, selectBuf);
                    //resultSet = selectConditionDouble(oidList,selectKey, selectBuf, resultSet);
                }else{
                    selectBufs.put(bufKey, selectBuf);
                }
            }
			selectKey = new String(columns[i]) + separator + "BOOL";
            bufKey = hybridBufs.get(selectKey);
			if(bufKey != null){
                selectBuf=searchBufs.get(bufKey);
                //found the right type
                if(bufKey.equals(selectKey)){
                    selectConditionBool(oidList,selectKey, selectBuf);
                    //resultSet = selectConditionBool(oidList,selectKey, selectBuf, resultSet);
                }else{
                    selectBufs.put(bufKey, selectBuf);
                }
            }
        } //end for
        for(String skey: selectBufs.keySet()){
            if(skey.equals(bufToSkip)){
                continue;
            }
            ByteBuffer buf = selectBufs.get(skey);
            selectFromBuf(columns, buf, skey, oidList);
            //resultSet = selectFromBuf(columns, resultSet, buf, skey, oidList);
        }
        return;
        //return resultSet;
    }
    
    protected void selectConditionString(List<Integer> oidList, String selectKey, ByteBuffer selectBuf){
        
//        HashMap <String, String> innerResults;
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
                
                /*stringReadBuf.position(pos);
                len = stringReadBuf.getInt();
                valstr = new byte[len];
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
        return;
        //return resultSet;
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
        return;
        //return resultSet;
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
        return;
        //return resultSet;
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

                /*
                 innerResults = resultSet.get(oid);
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
   
    public void printTable(String outputfile){
        try{
            File f = new File(outputfile);
            if(f.exists() && !f.isDirectory()) {
                f.delete();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));

            for(String keySet: searchBufs.keySet()){
                String[] keys = keySet.split("\\s+"); //separted by any white space
                ByteBuffer buf = searchBufs.get(keySet);
                int bound = buf.position();
                ByteBuffer readBuf = buf.asReadOnlyBuffer(), stringReadBuf = stringBuffer.asReadOnlyBuffer();
                readBuf.position(0);
                int oid = 0;
                long longnum = 0;
                double num = 0;
                byte[] valstr, valbool;
                int len;
                String[] types = new String[keys.length];
                
                for(int i=0; i<keys.length; i++){
                    String[] parts = keys[i].split(separator);
                    types[i] = parts[1];
                }
                
                bw.write(keySet+"  ");
                
                while(readBuf.position() < bound) {
                    oid = readBuf.getInt();
                    bw.write("id:" + oid + " ");
                    
                    for(int i=0; i<keys.length; i++){
                        if(types[i].equals("STRING")){
                            int pos = (int)readBuf.getLong();
                            if(pos == UNDEFINED){
                                bw.write("null");
                                continue;
                            }
                            stringReadBuf.position(pos);
                            len = stringReadBuf.getInt();
                            //if(len != UNDEFINED){
                            valstr = new byte[len];
                            stringReadBuf.get(valstr);
                            bw.write(new String(valstr) + " ");
                            //                        innerSet.put(keyType[columnIndex], new String(valstr));
                            //}
                        }else if(types[i].equals("LONG")){
                            longnum = readBuf.getLong();
                            if(longnum != UNDEFINED)
                                bw.write(String.valueOf(longnum) + " ");
                            //innerSet.put(keyType[columnIndex],  String.valueOf(longnum));
                        }else if(types[i].equals("DOUBLE")){
                            num = readBuf.getDouble();
                            if(num != UNDEFINED)
                                bw.write(String.valueOf(num) + " ");
                        }else if(types[i].equals("BOOL")){
                            valbool = new byte[1];
                            readBuf.get(valbool);
                            if(valbool[0] != UndefByte[0])
                                bw.write(String.valueOf(valbool[0]) + " ");
                            //innerSet.put(keyType[columnIndex], String.valueOf(valbool[0]));
                        }
                    }
                }
                bw.newLine();

            }
            
            bw.close();
            
        }catch (FileNotFoundException e){
            System.err.println("FileNotFoundException:"+e.getMessage());
            return ;
        } catch (IOException e){
            System.err.println("IOException:"+e.getMessage());
            return ;
        }
            
    }
    
    //TODO: finish this
	public int getObject(int targetId)
	{
        /* tranverse each buffer to read it *
        //get the buffer
        for(String skey: searchBufs.keySet()){
            String[] keys = skey.split("\\s+"); //separted by any white space
			// check the key and assign different method
            ByteBuffer buf = searchBufs.get(skey);
            int bound = buf.position();
            ByteBuffer readBuf = buf.asReadOnlyBuffer();
            readBuf.position(0);
            int oid = 0;
            long longnum = 0;
			byte [] key, valstr;
			int len;
            
            while(readBuf.position() < bound) {
                oid = readBuf.getInt();
                //System.out.print("current cursor: Rowid "+oid+" key "+skey);
                if(oid > targetId)
                    break; // already found all of the target row , assume monotonous increase
                if(oid == targetId)
                    System.out.print("Row "+oid);
                
                for(int i=0; i<keys.length; i++){
					String[] parts = keys[i].split(separator);
                	String type = parts[1];

                    if(type.equals("STRING")){
                    }
                    else if(type.equals("LONG")){
                        longnum = readBuf.getLong();
                        if(oid == targetId){
                            System.out.print(longnum + " ");
                        }
                    }
                    else if(type.equals("DOUBLE")){
                    }
                    else if(type.equals("BOOL")){
                    }
                }
                if(oid == targetId)
                    System.out.println("");
            } //end while
        } // end for*/
        return 0;
    }

    public void printBriefStats(){
        result.printBriefStats();
        return;
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
				//System.out.println("  OBJECT");
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						prepareRow(objid,object.get(name),key+"."+name);
					else
						prepareRow(objid,object.get(name),name);
				}
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
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
		for(String skey: searchBufs.keySet()){
            
            String[] keys = skey.split("\\s+"); //separted by any white space
            ByteBuffer buf = searchBufs.get(skey);
            boolean emptyRow = true;
            int numNullinRow = 0;
            
			//leave 1K as the warning threshold - don't insert after it
	    	if(buf.position() > buf.capacity() - 5000){
	    		//don't insert any more -- buffer almost full
	    		System.out.println("buffer is almost full. No inserts any more!");
	    		break;
	    	}
            int position = buf.position();
            //first put the objid
			buf.putInt(objid);
            
            for(int i=0; i<keys.length; i++){
				String[] parts = keys[i].split(separator);
                String type = parts[1];
                if(type.equals("STRING")){
					if(tempBufString.get(keys[i]) != null){
                        emptyRow = false;
						long pos = stringBuffer.position();
						buf.putLong(pos);
						stringBuffer.putInt(tempBufString.get(keys[i]).length());
						stringBuffer.put(tempBufString.get(keys[i]).getBytes());
                    	//buf.putInt(tempBufString.get(keys[i]).length());
					   	//buf.put(tempBufString.get(keys[i]).getBytes());
                        numOfDataPt++;
					} else{
						buf.putLong(UNDEFINED);
                        numNullinRow++;
                    }
                }
                else if(type.equals("LONG")){
                    if(tempBufLong.get(keys[i]) != null){
                        buf.putLong(tempBufLong.get(keys[i]));
                        emptyRow = false;
                        numOfDataPt++;
                    }
                    else{
                        buf.putLong(UNDEFINED);
                        numNullinRow++;
                    }
                }
                else if(type.equals("DOUBLE")){
					if(tempBufDouble.get(keys[i]) != null){
                        buf.putDouble(tempBufDouble.get(keys[i]));
                        emptyRow = false;
                        numOfDataPt++;
                    }
                    else{
                        buf.putDouble(UNDEFINED);
                        numNullinRow++;
                    }
                }
                else if(type.equals("BOOL")){
					if(tempBufBool.get(keys[i]) != null){
						String value = tempBufBool.get(keys[i]);
						if(value.equals("TRUE")==true){
							buf.put((byte)1);
						}else if(value.equals("FALSE")==true){
							buf.put((byte)0);
						}
                        emptyRow = false;
                        numOfDataPt++;
					}
					else{
						buf.put(UndefByte);
                        numNullinRow++;
                    }
                }
                else{
                    System.out.println("Error1: no such type in buf "+type);
                    return;
                }
            }
            
            if(emptyRow == true){
                buf.position(position);
            }else{
                numOfNull = numOfNull + numNullinRow;
                numOfRowId++;
            }
        }
    }

	public static void main(String[] args) throws IOException{
		/* flatten the json file */ 
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde3.json"));
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		RowColStoreEng parser= new RowColStoreEng(15*1000*1000*1000,"testjson/abcde3.layout");
		for(int objid = 0; objid < 100000; objid++){
            parser.insertObject(objid,jsonob,null);
        }
        byte[] where = "A".getBytes();

        byte[][] columns = new byte[4][];
        columns[0] = "A".getBytes();
        columns[1] = "B".getBytes();
        columns[2] = "P".getBytes();
        columns[3] = "Q".getBytes();
        //columns[4] = "E".getBytes();
        //columns[5] = "F".getBytes();
        //columns[6] = "G".getBytes();
        //columns[7] = "H".getBytes();
        //columns[8] = "I".getBytes();
        
        for(int i = 0; i < 10; i++){
            long start = System.currentTimeMillis();
            //HashMap<Integer, String> resultSet = parser.select(columns);
            parser.selectRange(columns,where,0,100);
            long end = System.currentTimeMillis();
            System.out.print((end-start) + " ");
        }
        System.out.println();
        parser.printBriefStats();
	}
}
