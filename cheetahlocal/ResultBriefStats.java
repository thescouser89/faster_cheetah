import java.util.HashMap;
import java.util.HashSet;



public class ResultBriefStats {
    //public int resultSetSize;
    public int index;
    //public int size;
    public int[] oids;
    
    public ResultBriefStats (int index, int[] oids) {
        //this.resultSetSize = resultSetSize;
        this.index = index;
        //this.size = size;
        this.oids = oids.clone();
    }
    
//    public int getResultsSetSize() {
//        return this.resultSetSize;
//    }
    
    public int getIndex() {
        return this.index;
    }
    
//    public int getSize() {
//        return this.size;
//    }
    
    public int[] getOids() {
        return this.oids;
    }
    
    public void printBriefStats(){
        HashSet<Integer> resultSet = new HashSet<Integer>();
        for(int i = 0; i < index; i++){
                resultSet.add(oids[i]);
        }
        System.out.print("selected " + resultSet.size() + " objects with " + index + " fields");
        return;
    }
    
    
    public String makeBriefStatsString() {
        HashSet<Integer> resultSet = new HashSet<Integer>();
        for(int i = 0; i < index; i++){
                resultSet.add(oids[i]);
        }
       return new String("selected " + resultSet.size() + " objects with " + index + " fields");
    }
}
