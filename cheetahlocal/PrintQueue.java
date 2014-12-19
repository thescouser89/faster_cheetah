import java.lang.Thread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Class to print finished queries
 * This is intended to run on one thread only
 * Initialized by SimpleQueryExecutor
 * and instance should be passed to QueryParallel
 *
 */
public class PrintQueue extends Thread{
    
    private String queryfile;
    private String[] splitQueryFile;
    private String storeMethod;
    
    public static ConcurrentLinkedQueue<QueryParallel> printerQueue = new ConcurrentLinkedQueue<QueryParallel>();
    public static int numberOfQueries = 0;    
    private static int numberPrinted = 0;    
    
    private static final int INITIAL_SLEEP_TIME = 5000;
    
    public PrintQueue(String queryfile, String storeMethod) {
        this.queryfile = queryfile;
        this.splitQueryFile = queryfile.split("\\/");
        this.storeMethod = storeMethod;
    }
    
    
    //works for NewColStoreEngParallel
    void printSummary(QueryParallel query){
            String[] temp = query.getQueryString().split(" ");
            int sel = (Integer.parseInt(temp[temp.length - 1])) / 100;
            
            System.out.print(splitQueryFile[splitQueryFile.length - 1] + "_selectivity:" + sel + "%;" + storeMethod);
            System.out.print(";");
            query.printBriefStatsString();
            //query.printBriefStats();
            System.out.print(";" + query.getRunTime());
    }
    
    @Override
    public void run() {
        System.out.println("Print threadid:" + Thread.currentThread().getId());
        while( numberOfQueries == 0 ) ;//{
//            try {
//                Thread.sleep(INITIAL_SLEEP_TIME);
//            } catch (Exception e ) {
//                System.out.println("Thread sleep failed: " + e.getMessage());
//            }
//        }
        while( numberOfQueries > numberPrinted ) {
            QueryParallel query = printerQueue.poll();
            if( query == null ){
                continue;
            }
            String[] temp = query.getQueryString().split(" ");
            int sel = (Integer.parseInt(temp[temp.length - 1])) / 100;
            
            System.out.print(splitQueryFile[splitQueryFile.length - 1] + "_selectivity:" + sel + "%;" + storeMethod);
            System.out.print(";");
            query.printBriefStatsString();
            //query.printBriefStats();
            System.out.print(";" + query.getRunTime());
            
            System.out.println("");
            numberPrinted++;
            //System.out.println("\nnumber printed"+ numberPrinted);
        }
    }
    
}
