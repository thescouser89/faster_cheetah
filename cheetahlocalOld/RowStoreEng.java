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

public class RowStoreEng extends StoreEngine {
	private ByteBuffer buffer; //save all of the contents
	private ByteBuffer stringBuffer;
    private static String [] fields;
	private static String [] types;
    private static Hashtable <String, Long> tempBufLong = new Hashtable<String, Long>(); 
    private static Hashtable <String, Double> tempBufDouble = new Hashtable<String, Double>(); 
    private static Hashtable <String, String> tempBufString = new Hashtable<String, String>();
    private static Hashtable <String, String> tempBufBool = new Hashtable<String, String>();
    private int numOfNull;
    private int numOfDataPt;
    private int numOfRowId;
    
    /**
     * Creates a ring buffer
	 * initialize the buffer size 
     */
    public RowStoreEng (int memory_size_in_bytes, String defFile)
    {
		//int max_buf_size = memory_size_in_bytes; 
		super(memory_size_in_bytes);
		// create a byte buffer 
		buffer = ByteBuffer.allocateDirect(max_buf_size);
        stringBuffer = ByteBuffer.allocateDirect(max_buf_size/5);
        numOfDataPt = 0;
        numOfNull = 0;
        numOfRowId = 0;
        
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(defFile));
            String line  = bufferedReader.readLine();
			String[] temp = line.split("\\s+");
			fields = new String[temp.length];
			types = new String[temp.length];
			for(int i=0; i<temp.length; i++){
				String[] temp2 = temp[i].split(":");
				fields[i] = temp2[0];
				types[i] = temp2[1];
			}
			//fields = temp.split(":")[];
			//fields = line.split("\\s+"); //separted by any white space 
            bufferedReader.close();
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
        return (double)numOfNull / (double)numOfDataPt;
    }

    public int getRow(int targetId)
	{
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
		int oid = 0;
		long longnum=0;
		double doublenum=0;
		byte [] valstr, valbool;
		int len;

		while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();
			if(oid > targetId)
				break; /* found the target row - assume monotonous increase */
			if(oid == targetId)
				System.out.print("Row "+oid);
		
			for(int i = 0; i < fields.length; i++){
				//String [] parts = fields[i].split(separator);
				//String columnKey = parts[0];
	            String type = types[i];
	            
	            if(type.equals("STRING")){
					len = readBuf.getInt();
					if(len != UNDEFINED){
						valstr = new byte[len];
						readBuf.get(valstr);
						if(oid == targetId)
							System.out.print(" " + new String(valstr));
					}
					else{
						if(oid == targetId)
							System.out.print(" null");
					}
	            }else if(type.equals("LONG")){
	            	longnum = readBuf.getLong();
	            	if(oid == targetId){
						if(longnum != UNDEFINED)
		    				System.out.print(" "+longnum);
						else
							System.out.print(" null");
					}
	            }else if(type.equals("DOUBLE")){
	            	doublenum = readBuf.getDouble();
	            	if(oid == targetId){
						if(doublenum != UNDEFINED)
		            		System.out.print(" "+doublenum);
						else
							System.out.print(" null");
                    }
	            }else if(type.equals("BOOL")){
                    valbool = new byte[1];
                    readBuf.get(valbool);
                    if(oid == targetId){
                        if(valbool != UndefByte){
							System.out.print(" " + String.valueOf(valbool[0]));
                        }else{
                            System.out.print(" null");
                        }
                    }
	            }else{
	                System.out.println("Error: no such type in buf "+type);
	                return 1;
	            }
			}
			if(oid == targetId)
				System.out.println("");
		} /* end while */
        return 0;
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
            // check the size
            //if((objid % 10000) == 1) 
           //    System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
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
			//saveStrValue(objid,key,st.getString());
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
		//leave 1K as the warning threshold - don't insert after it 
	    if(buffer.position() > buffer.capacity() - 1000){
	    	//don't insert any more -- buffer almost full
	    	System.out.println("buffer is almost full. No inserts any more!");
	    	return;
	    }
	    //first put the objid
		buffer.putInt(objid);
        numOfRowId++;

		for(int i = 0; i < fields.length; i++){
			//String [] parts = fields[i].split(separator);
			String key = fields[i];
            String type = types[i];
            String keyType = key+separator+type;
//            System.out.print(buffer.position() + " ");
            if(type.equals("STRING")){
				if(tempBufString.get(keyType) != null){
					//buffer.putInt(tempBufString.get(keyType).length());
    				//buffer.put(tempBufString.get(keyType).getBytes());
					long position = stringBuffer.position();
					buffer.putLong(position);
					stringBuffer.putInt(tempBufString.get(keyType).length());
    				stringBuffer.put(tempBufString.get(keyType).getBytes());
                    numOfDataPt++;
				}
    			else{
                    buffer.putLong(UNDEFINED);
                    numOfNull++;
                }
					//buffer.putInt(UNDEFINED);  //a null value
            }
            else if(type.equals("LONG")){
    			if(tempBufLong.get(keyType) != null){
    				buffer.putLong(tempBufLong.get(keyType));
                    numOfDataPt++;
                }
    			else{
					buffer.putLong(UNDEFINED);
                    numOfNull++;
                }
            }
            else if(type.equals("DOUBLE")){
            	if(tempBufDouble.get(keyType) != null){
            		buffer.putDouble(tempBufDouble.get(keyType));
                    numOfDataPt++;
                }
    			else{
					buffer.putDouble(UNDEFINED);
                    numOfNull++;
                }
            }
            else if(type.equals("BOOL")){
                if(tempBufBool.get(keyType) != null){
                    String value = tempBufBool.get(keyType);
                    if(value.equals("TRUE")==true){
                        buffer.put((byte)1);
                    }else if(value.equals("FALSE")==true){
                        buffer.put((byte)0);
                    }
                    numOfDataPt++;
                }
                else{
                    buffer.put(UndefByte);
                    numOfNull++;
                }
            }
            else{
                System.out.println("Error: no such type in buf "+type);
                return;
            }
			//if(objid == 0){
			//	System.out.print(i + ":"+buffer.position()+" ");
			//}
		}
//		System.out.println();
	}
    
    public HashMap<Integer, HashMap<String, String>> selectOld(byte[][] columns){
        HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
        String[] keyType = new String[fields.length];
        int[] selectFields = new int[fields.length];
        boolean containColumn = false;
        
		for(int i = 0; i < fields.length; i++){
			selectFields[i] = 0;
			String columnKey = fields[i];
            keyType[i] = fields[i]+separator+types[i];
			for(int j = 0; j < columns.length; j++){
	 			if(Arrays.equals(columnKey.getBytes(), columns[j])==true){
					selectFields[i] = 1;
                    containColumn = true;
				}
			}
		}
        
        //This buffer does not contain any of the target columns
        if(containColumn == false){
            return resultSet;
        }
        
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long longnum = 0;
        double num = 0;
        int oid=0;
        int len=0;
        byte[] valstr, valbool;
		int position = 0;
        
		while(readBuf.position()<bound){
            oid=readBuf.getInt();
            position += 4;
            HashMap <String, String> innerSet = new HashMap<String, String>();
            
            for(int i = 0 ; i < fields.length; i++){
                //String keyType = fields[i]+":"+types[i];
                String type = types[i];
                if(type.equals("STRING")){
                    readBuf.position(position);
                    len=readBuf.getInt();
                    position += 4;
                    
                    if(len != UNDEFINED){
                        if(selectFields[i] == 1){
                            valstr = new byte[len];
                            readBuf.get(valstr);
                            innerSet.put(keyType[i], new String(valstr));
                        }
                        position += len;
                    }
                }
                else if(type.equals("LONG")){
                    if(selectFields[i] == 1){
                        readBuf.position(position);
                        longnum = readBuf.getLong();
                        if(longnum != UNDEFINED)
                            innerSet.put(keyType[i],  String.valueOf(longnum));
                    }
                    position += 8;
                }
                else if(type.equals("DOUBLE")){
                    if(selectFields[i] == 1){
                        readBuf.position(position);
                        num = readBuf.getDouble();
                        if(num != UNDEFINED)
                            innerSet.put(keyType[i], String.valueOf(num));
                    }
                    position += 8;
                }
                else if(type.equals("BOOL")){
                    if(selectFields[i] == 1){
                        valbool = new byte[1];
                        readBuf.position(position);
                        readBuf.get(valbool);
                        if(valbool != UndefByte)
                            innerSet.put(keyType[i], String.valueOf(valbool[0]));
                    }
                    position += 1;
                }
			}
            readBuf.position(position);
            if(!innerSet.isEmpty()){
                resultSet.put(oid, innerSet);
            }
		}
		return resultSet;
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
    
    public HashMap<Integer, HashMap<String, String>> select(byte[][] columns){
        HashMap<Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
        String[] keyType = new String[fields.length];
        int[] skip = new int[fields.length];
        int[] columnIndexes = new int[fields.length];
        int count = 0, index = 0;
        
		for(int i = 0; i < fields.length; i++){
            keyType[i] = fields[i]+separator+types[i];
			boolean fieldSelected = false;
            for(int j = 0; j < columns.length; j++){
	 			if(Arrays.equals(fields[i].getBytes(), columns[j])==true){
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
        
        //This buffer does not contain any of the target columns
        if(index == 0){
            return resultSet;
        }
        ByteBuffer readBuf = buffer.asReadOnlyBuffer(), stringReadBuf = stringBuffer.asReadOnlyBuffer();
        int bound = buffer.position(), oid=0, len = 0, newpos;
        long longnum = 0;
        double num = 0;
        byte[] valstr, valbool;
        readBuf.position(0);
		stringReadBuf.position(0);
        HashMap <String, String> innerSet;
        
		while(readBuf.position()<bound){
            oid=readBuf.getInt();
            innerSet = new HashMap<String, String>();
            
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
                    if(len != UNDEFINED){
                        valstr = new byte[len];
                        stringReadBuf.get(valstr);
                        innerSet.put(keyType[columnIndex], new String(valstr));
                    }
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
                    if(valbool != UndefByte)
                        innerSet.put(keyType[columnIndex], String.valueOf(valbool[0]));
                }
            }
            if(skipToRowEnd != 0){
                newpos = readBuf.position() + skipToRowEnd;
                readBuf.position(newpos);
            }
            if(!innerSet.isEmpty()){
                resultSet.put(oid, innerSet);
            }
		}
		return resultSet;
	}

    public HashMap<Integer, HashMap<String, String>> selectWhere(byte[][] selectCols, byte[] whereCol, String selectType, long value1, long value2, String relation, byte[] conditionValue){
        
        HashMap <Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
		String[] keyType = new String[fields.length];
        
        int[] skip = new int[fields.length], columnIndexes = new int[fields.length], whereSkip = new int[fields.length], whereIndexes = new int[fields.length];
        int count = 0, count2 = 0, index = 0, index2 = 0;
        
		for(int i = 0; i < fields.length; i++){
            keyType[i] = fields[i]+separator+types[i];
			boolean isSelectField = false, isWhereField = false;;
			if(selectCols[0][0]==(byte) '*'){
				columnIndexes[index] = i;
				skip[index] = count;
				index++;
				isSelectField = true;
				count = 0;
			}else{
        	    for(int j = 0; j < selectCols.length; j++){
		 			if(Arrays.equals(fields[i].getBytes(), selectCols[j])==true){
	                    columnIndexes[index] = i;
                	    skip[index] = count;
              	    	index++;
                    	count = 0;
						isSelectField = true;
					}
				}
			}

            if(selectType.equals("selectWhereAny")){
				String regex = "^" + new String(whereCol) + "\\[\\d*\\]";
				if((fields[i].matches(regex))&&(types[i].equals("STRING"))) {
//					System.out.println(i);
                    whereIndexes[index2] = i;
                    whereSkip[index2] = count2;
                    index2++;
                    count2 = 0;
					isWhereField = true;
				}
			}else if(selectType.equals("selectRange")){
				if((Arrays.equals(fields[i].getBytes(), whereCol)==true) && (types[i].equals("LONG"))){
					whereIndexes[index2] = i;
                    whereSkip[index2] = count2;
                    index2++;
                    count2 = 0;
					isWhereField = true;
				}
			}else if(selectType.equals("selectWhereSingle")){
				if((Arrays.equals(fields[i].getBytes(), whereCol)==true) &&(types[i].equals("STRING"))){
					whereIndexes[index2] = i;
                    whereSkip[index2] = count2;
                    index2++;
                    count2 = 0;
					isWhereField = true;
				}
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
        int skipToRowEnd = count;
        int lastWhereToEnd = count2;
        
		//This buffer does not contain any of the where or select columns
        if((index == 0) || (index2 == 0)){
			return resultSet;
        }
        
        ByteBuffer readBuf = buffer.asReadOnlyBuffer(), stringReadBuf = stringBuffer.asReadOnlyBuffer();
        int bound = buffer.position(), oid=0, len = 0, newpos;
        long longnum = 0, value;
        double num = 0;
        byte[] valstr, valbool;
        byte[] conditionStr = conditionValue;
        boolean conditionFlag;
        readBuf.position(0);
        int rowStartPosition = 0;
        
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
                if(selectType.equals("selectRange")){
					value = readBuf.getLong();
					if(value != UNDEFINED){
						if( (value >= value1) && (value <= value2)){
                			conditionFlag = true;
            			}
              		}
				} else{
					int pos = (int)readBuf.getLong();
					if(pos == UNDEFINED){
						continue;
					}
					if(conditionFlag == true){
						continue;
					}
                	stringReadBuf.position(pos);
					len = stringReadBuf.getInt();
                   	//len=readBuf.getInt();
					if(len != UNDEFINED){
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
                }
            }
            
            if(conditionFlag){
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
                        if(len != UNDEFINED){
                            valstr = new byte[len];
                            stringReadBuf.get(valstr);
                            innerSet.put(keyType[columnIndex], new String(valstr));
                        }
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
		}
		return resultSet;
    }

    public HashMap<Integer, HashMap<String, String>> selectWhereOld(byte[][] selectCols, byte[] whereCol, String selectType, long value1, long value2, String relation, byte[] conditionValue){
        HashMap <Integer, HashMap<String, String>> resultSet = new HashMap<Integer, HashMap<String, String>>();
		String[] keyType = new String[fields.length];
		int[] selectFields = new int[fields.length], whereFields = new int[fields.length];
	    boolean containSelectCols = false, containWhereCols = false;
	    int lastWhereIndex = 0;

		for(int i = 0; i < fields.length; i++){
			selectFields[i] = 0;
			String columnKey = fields[i];
            keyType[i] = fields[i]+separator+types[i];
			for(int j = 0; j < selectCols.length; j++){
	 			if(Arrays.equals(columnKey.getBytes(), selectCols[j])==true){
					selectFields[i] = 1;
					containSelectCols = true;
				}
			}
			if(selectType.equals("selectWhereAny")){
				String regex = "^" + new String(whereCol) + "\\[\\d*\\]";
				if((columnKey.matches(regex))&&(types[i].equals("STRING"))) {
					whereFields[i] = 1;
					lastWhereIndex = i;
					containWhereCols = true;
				}
			}else if(selectType.equals("selectRange")){
				if((Arrays.equals(columnKey.getBytes(), whereCol)==true) && (types[i].equals("LONG"))){
					whereFields[i] = 1;
					lastWhereIndex = i;
					containWhereCols = true;
				}
			}else if(selectType.equals("selectWhereSingle")){
				if((Arrays.equals(columnKey.getBytes(), whereCol)==true) &&(types[i].equals("STRING"))){
					whereFields[i] = 1;
					lastWhereIndex = i;
					containWhereCols = true;
				}
			}
		}
        
		//This buffer does not contain any of the where or select columns
        if((containSelectCols == false) && (containWhereCols == false)){
			return resultSet;
        }

		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        readBuf.position(0);
		long longnum = 0, value;
        double num = 0;
        int oid=0, len=0, bound = buffer.position(), position = 0, rowStartPosition = 0;
        byte[] valstr, valbool;
        byte[] conditionStr = conditionValue;
		boolean conditionFlag, whereColChecked;
		HashMap <String, String> innerSet;
        
		while(readBuf.position()<bound){
            oid=readBuf.getInt();
			position += 4;
			rowStartPosition = position;
			conditionFlag = false;
			//quickly skip to the where col to check its condition
            for(int i = 0 ; i < (lastWhereIndex + 1); i++){
				//System.out.print(i+" ");
				String type = types[i];
				if((whereFields[i] != 1) || (conditionFlag == true)){//if this col is not the where col, just skip it
				//	System.out.print(keyType[i] + "|" + position+ "-");
					position = skipNext(readBuf,position, type);
			//		System.out.print(position+" ");
					continue;
				} 
				//if(whereFields[i] == 1)
					//System.out.print(i + " is the where Col");
				//if this col is the where col, read it and check the condition
				//check condition differently depending on the type
                if(selectType.equals("selectRange")){
					readBuf.position(position);
					value = readBuf.getLong();
                    position += 8;
					if(value != UNDEFINED){
						if( (value >= value1) && (value <= value2)){
                			conditionFlag = true;
            			}
              		}
				} else{
					readBuf.position(position);
                   	len=readBuf.getInt();
                   	position += 4;
					if(len != UNDEFINED){
						conditionFlag = true;
                        position += len;
                        
                        // if the length is not equal, skip the check statement, and set it as false
                        if (len != conditionStr.length){
                            conditionFlag = false;
                            continue;
                        }
                        
                        valstr = new byte[len];
                        readBuf.get(valstr);

						for(int j = 0; j < len; j++){
                            if(valstr[j] != conditionStr[j]){
    	               			conditionFlag = false;
    	               			break;
    	           			}
    	       			}
                        //System.out.println(keyType[i] + new String(valstr));
                	}
                }
			}

			//System.out.println("done checking current position is " + position + " " + readBuf.position());
			//after checking the where col:
			if(conditionFlag){
				//System.out.println("1");
                position = rowStartPosition;
				innerSet = new HashMap<String, String>();
				//if it's SELECT *
				if(selectCols[0][0]==(byte) '*'){
					readBuf.position(position);
					for(int i = 0 ; i < fields.length; i++){
                		if(types[i].equals("STRING")){
                    		len=readBuf.getInt();
							if(len != UNDEFINED){
                            	valstr = new byte[len];
                           		readBuf.get(valstr);
                           		innerSet.put(keyType[i], new String(valstr));
                    		}
						}else if(types[i].equals("LONG")){
                        	longnum = readBuf.getLong();
                        	if(longnum != UNDEFINED)
                            	innerSet.put(keyType[i],  String.valueOf(longnum));
                		}else if(types[i].equals("DOUBLE")){
                        	num = readBuf.getDouble();
                        	if(longnum != UNDEFINED)
                            	innerSet.put(keyType[i], String.valueOf(num));
                		}else if(types[i].equals("BOOL")){
                        	valbool = new byte[1];
                        	readBuf.get(valbool);
                        	if(valbool != UndefByte)
                            	innerSet.put(keyType[i], String.valueOf(valbool[0]));
                		}
					}
				}
                else{
                    for(int i = 0 ; i < fields.length; i++){
                        if(selectFields[i] != 1){
                            position = skipNext(readBuf, readBuf.position(), types[i]);
                            continue;
                        }
                        readBuf.position(position);
                        if(types[i].equals("STRING")){
                            len=readBuf.getInt();
                            if(len != UNDEFINED){
                                valstr = new byte[len];
                                readBuf.get(valstr);
                                innerSet.put(keyType[i], new String(valstr));
                            }
                        }else if(types[i].equals("LONG")){
                            longnum = readBuf.getLong();
                            if(longnum != UNDEFINED)
                                innerSet.put(keyType[i],  String.valueOf(longnum));
                        }else if(types[i].equals("DOUBLE")){
                            num = readBuf.getDouble();
                            if(num != UNDEFINED)
                                innerSet.put(keyType[i], String.valueOf(num));
                        }else if(types[i].equals("BOOL")){
                            valbool = new byte[1];
                            readBuf.get(valbool);
                            if(valbool != UndefByte)
                                innerSet.put(keyType[i], String.valueOf(valbool[0]));
                        }
                    }
                }
                if(!innerSet.isEmpty()){
                    resultSet.put(oid, innerSet);
                }
                position = readBuf.position();
			} else{//if this row is not selected, just skip the entire row
				//System.out.print("time to skip from " + position + " lastwhereindex: " + lastWhereIndex);
				//System.out.println("2 " + position);
				
				for(int i = (lastWhereIndex+1); i < fields.length; i++){
					//System.out.print(i + ":"+position+" ");
					position = skipNext(readBuf, position, types[i]);
					//System.out.print(i + ":"+position+" ");
				}
				//System.out.print("end: "+position+" ");
                readBuf.position(position);
			}
		}
		return resultSet;
    }
    
    
	/* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public HashMap<Integer, HashMap<String, String>> selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2){
        HashMap<Integer, HashMap<String, String>> resultSet = selectWhere(selectCols, whereCol, "selectRange", value1, value2, "", null);
    	return resultSet;
    }

	/* select x,y,z where a = ANY xx
     * xx is a set, single column and single relation parsing
     * Method:  there are multiple columns for this set xx,
     *         scan all of where columns and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     *        assume string type for now
     */
    public HashMap<Integer, HashMap<String, String>>  selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
    	HashMap<Integer, HashMap<String, String>> resultSet = selectWhere(selectCols, whereCol, "selectWhereAny", 0, 0, relation, value);
    	return resultSet;
	}
                        
    /* select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing 
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
	public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
		HashMap<Integer, HashMap<String, String>> resultSet = selectWhere(selectCols, whereCol, "selectWhereSingle", 0, 0, relation, value);
    	return resultSet;
	}

    public long aggregateColumn(byte[] selectCol, byte[] whereCol, int threshold){
        /* assume we all have the same type , but need to check the column number*/
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long rowsum = 0;
		long sum = 0;
        long longnum = 0;
        int oid;
        int whereField = -1; //the column that check the selectivity
		int selectField = -1; //the column to be selected, if condition is met

        for(int i = 0 ; i < fields.length; i++){
			String columnKey = fields[i];
            if(Arrays.equals(columnKey.getBytes(), selectCol)==true){
                selectField = i;
                //break;
            }
			if(Arrays.equals(columnKey.getBytes(), whereCol)==true){
                whereField = i;
            }
        }

        if(whereField == -1){
            System.out.println("Error: no where field.");
            return -1;
        }
		if(selectField == -1){
            System.out.println("Error: no select field.");
            return -1;
        }

        while(readBuf.position()<bound){
            rowsum = 0;
			oid = readBuf.getInt();
            for(int i = 0 ; i < fields.length; i++){
				longnum = readBuf.getLong();
                if(i == whereField){
                    // check the selectivity
                    if(longnum>= threshold){ // not selected
                        int oldpos = readBuf.position();
                        int newpos= oldpos+(fields.length-i-1)*8; // one long
                        readBuf.position(newpos); //skip this row
                        rowsum = 0; //reset rowsum
                        break;
                    }
                }
				if(i == selectField){
                	rowsum += longnum;
				}
            }
			sum += rowsum;
        }
        return sum;
    }


    public long fastAggregate(byte[] colName,int threshold){
        /* assume we all have the same type , but need to check the column number*/
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long rowsum = 0;
        long longnum = 0;
        int oid;
        int colField = -1; //the column that check the selectivity
        for(int i = 0 ; i < fields.length; i++){
			String columnKey = fields[i];
            if(Arrays.equals(columnKey.getBytes(), colName)==true){
                colField = i;
                break;
            }
        }
        if(colField == -1){
            System.out.println("Error: no select field.");
            return -1;
        }


        while(readBuf.position()<bound){
            oid = readBuf.getInt();
            for(int i = 0 ; i < fields.length; i++){
                longnum = readBuf.getLong();
                if(i == colField){
                    // check the selectivity
                    if(longnum>= threshold){ // not selected
                        int oldpos = readBuf.position();
                        int newpos= oldpos+(fields.length-i-1)*8; // one long
                        readBuf.position(newpos); //skip this row
                        rowsum = 0; //reset rowsum
                        break;
                    }
                }
                rowsum += longnum;
            }
        }
        return rowsum;

    }

    public long fastAggregate1(byte[] colName,int threshold){
        /* assume we all have the same type */
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long rowsum = 0;
        long longnum = 0;
        int oid;
        //assume selectivity is 1 for now
        //simple sum all of them
        while(readBuf.position()<bound){
            oid = readBuf.getInt();
            for(int i = 0 ; i < fields.length; i++){
	       	    longnum = readBuf.getLong();
                rowsum += longnum;
            }
        }
        return rowsum;
            
 
    }


	public long aggregate(byte[] colName,int threshold){
    	ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
        long rowsum = 0;
        long sum=0;
		long longnum=0;
		double doublenum=0;
		int oid = 1;
        boolean selectFlag = false; //indicate whether this row is selected or not, based on the condition
        //boolean fieldFlag = false; //indicate whether this field is the conditional check field or not
		String column = new String(colName);

        while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();

			for(int i = 0; i < fields.length; i++){
				String columnKey = fields[i];
				String type = types[i];
	
	            //if(Arrays.equals(columnKey.getBytes(), colName)==true){
	            //	fieldFlag = true;
	            //}else{
	            //	fieldFlag = false;
	            //}
 
	            if(type.equals("STRING")){
	            }else if(type.equals("LONG")){
	            	longnum = readBuf.getLong();
	            	//if(fieldFlag==true){
	                if(columnKey.equals(column)){  
						  //check selectivity 
	                    if(longnum <= threshold){
	                        selectFlag = true; 
	                    } 
	                }
	                rowsum += longnum;
	            }else if(type.equals("DOUBLE")){
	            	doublenum = readBuf.getDouble();
	            }else if(type.equals("BOOL")){
	            }else{
	                System.out.println("Error: no such type in buf "+type);
	                return 1;
	            }
			}
	
            if(selectFlag==true){
            	sum += rowsum;
              //  System.out.println("row sum value "+rowsum+" sum "+sum);
            }
                //System.out.println("reset rowsum,selectFlag");
                // reset row stats and selectFlag -- must happen before we check the key
            rowsum = 0;
            selectFlag = false;
		} /* end while */
        return sum;
    }
    
	public static void main(String[] args) throws IOException{
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde2.json")); 

		//Assuming we know all the columns and data type in advance
		//fields = new String[] {"A:LONG","B:LONG","C:LONG","D:LONG","E:DOUBLE"};
		
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		RowStoreEng store= new RowStoreEng(100*1000*1000, "testjson/abcde2_definition");
		
		//insert objects
		for(int objid=1; objid<=10; objid++){
			store.insertObject(objid,jsonob,null);
		}

		System.out.println("get the result out \n");
		store.getRow(0);
		// aggregate scan   
        String targetColumn = "D";
        long sum = store.aggregate(targetColumn.getBytes(),500);
        System.out.println("Aggregate sum Results : "+sum);
        sum = store.fastAggregate(targetColumn.getBytes(),2);
        System.out.println("Aggregate sum Results : "+sum);
	}
}
