import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Restart solution that avoids starvation (live locks).
 *
 * Idea (three layers):
 *  1. Aborted transactions are restarted from the client side (as in RestartBlocking).
 *  2. Before a restart the task sleeps for a random, exponentially growing time (backoff),
 *     so that the same tasks do not collide again and again.
 *  3. A task that failed "escalate_after" times in a row is escalated: it takes the write lock
 *     of a FAIR (FIFO) read/write gate and runs completely alone. Normal tasks run in parallel
 *     under the read lock. Because nobody else runs in the meantime, the escalated task can not
 *     deadlock any more and is guaranteed to commit. Every task therefore needs at most
 *     escalate_after failed attempts plus one exclusive attempt.
 *
 */
public class NoLiveLocks {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;
    public static int escalate_after = 3;

    static final long BASE_BACKOFF_MS = 20;
    static final long MAX_BACKOFF_MS = 500;

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;

    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger total_retries = new AtomicInteger(0);
    AtomicInteger max_retries = new AtomicInteger(0);
    AtomicInteger escalated_tasks = new AtomicInteger(0);

    // fair = FIFO. Read lock = normal parallel execution, write lock = exclusive (starving task).
    final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);

    void executeSQL(String sql, Connection con) {
        try {
            java.sql.Statement stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing " + sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void cerateDB() {
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",
                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }
            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println("Inserted " + results.length + " rows");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //Create DB
        NoLiveLocks b = new NoLiveLocks();
        b.cerateDB();

        //Create task and their queries
        ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
        for (int task = 0; task < num_taks; task++) {
            my_tasks.add(b.new Blocker(task));
        }

        //Execute tasks
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
        double start = System.currentTimeMillis();
        for (Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        //wait for the tasks to finish
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        double stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly " + (stop - start)
                + " ms finished: " + b.tasks_finished + " of " + num_taks);
        System.out.println("Total retries: " + b.total_retries
                + ", max retries for one task: " + b.max_retries
                + ", tasks that needed exclusive mode: " + b.escalated_tasks);
    }

    public class Blocker implements Runnable {
        final int task;
        final double[] start_range;
        final double[] stop_range;
        final double[] new_value;

        public Blocker(int id) {
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range = new double[num_queries_per_task];
            this.new_value = new double[num_queries_per_task];
            for (int q = 0; q < num_queries_per_task; q++) {
                stop_range[q] = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
                new_value[q] = rand.nextDouble() * max_value;
            }
        }

        @Override
        public void run() {
            int attempts = 0;
            boolean committed = false;

            while (!committed) {
                // after too many failures this task runs ALONE (write lock), otherwise in parallel (read lock)
                boolean exclusive = attempts >= escalate_after;
                Lock lock = exclusive ? gate.writeLock() : gate.readLock();
                lock.lock();

                Connection con = null;
                try {
                    con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                    con.setAutoCommit(false);
                    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    PreparedStatement ps = con.prepareStatement(
                            "UPDATE blocking_data SET data_value = ?, modified_by = " + task +
                                    " WHERE data_value >= ? AND data_value <= ?;");
                    for (int q = 0; q < num_queries_per_task; q++) {
                        ps.setDouble(1, new_value[q]);
                        ps.setDouble(2, start_range[q]);
                        ps.setDouble(3, stop_range[q]);
                        ps.execute();
                    }
                    con.commit();
                    committed = true;

                    total_retries.addAndGet(attempts);
                    max_retries.accumulateAndGet(attempts, Math::max);
                    if (attempts >= escalate_after) escalated_tasks.incrementAndGet();
                    int n = tasks_finished.incrementAndGet();
                    System.out.println("Done task " + task + " after " + attempts + " retries"
                            + (attempts >= escalate_after ? " (exclusive)" : "")
                            + ". Finisher number " + n);
                } catch (SQLException e) {
                    attempts++;
                    try {
                        if (con != null) con.rollback();
                    } catch (SQLException ignore) {
                    }
                    String state = e.getSQLState();
                    if (!"40P01".equals(state) && !"40001".equals(state)) {
                        System.err.println("Task " + task + " fatal: " + e.getMessage());
                        return;
                    }
                } finally {
                    try {
                        if (con != null) con.close();
                    } catch (SQLException ignore) {
                    }
                    lock.unlock();//always release the gate, also before sleeping
                }

                // random exponential backoff, done OUTSIDE the lock
                if (!committed) {
                    long cap = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS << Math.min(attempts, 10));
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(cap + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }
}
