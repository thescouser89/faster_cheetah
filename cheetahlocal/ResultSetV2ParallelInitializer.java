import java.lang.Runnable;
import java.util.concurrent.Callable;

public class ResultSetV2ParallelInitializer implements Runnable, Callable<Long>{

    StoreEngine store;

    public ResultSetV2ParallelInitializer(StoreEngine store) {
        this.store = store;
    }

    @Override
    public Long call() {
        long tId = Thread.currentThread().getId();
        // store.addNewResultSet(tId);
        return tId;
    }

    @Override
    public void run() {
        long tId = Thread.currentThread().getId();
        store.addNewResultSet(tId);

    }
}
