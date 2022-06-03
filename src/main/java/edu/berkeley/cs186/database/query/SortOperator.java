package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;
import java.util.stream.Collectors;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double) numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        List<Record> r = new ArrayList<>();
        while (records.hasNext()) r.add(records.next());
        r.sort(comparator);
        Run run = makeRun();
        run.addAll(r);
        return run;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be added to the output run
     * next.
     * <p>
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        List<Iterator<Record>> runsIterator = runs.stream().map(Run::iterator).collect(Collectors.toList());

        PriorityQueue<Pair<Record, Integer>> mergeQueue = new PriorityQueue<>(runs.size(), new RecordPairComparator());

        // lấy phần tử đầu tiên của từng run
        for (int i = 0; i < runsIterator.size(); i++) {
            mergeQueue.offer(new Pair<>(runsIterator.get(i).next(), i));
        }

        Run result = makeRun();
        while (true) {
            // phần tử nhỏ nhất trong queue
            Pair<Record, Integer> currentPair = mergeQueue.poll();
            if (currentPair == null) break;

            Record currentRecord = currentPair.getFirst();
            int runIdx = currentPair.getSecond();

            result.add(currentRecord);
            Iterator<Record> currentIterator = runsIterator.get(runIdx);
            if (currentIterator.hasNext()) mergeQueue.offer(new Pair<>(currentIterator.next(), runIdx));
        }
        return result;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        int totalRunSize = runs.size();
        int totalPass = (int) Math.ceil(totalRunSize / (numBuffers - 1f));
        List<Run> result = new ArrayList<>();
        for (int i = 0; i < totalPass; i++) {
            int start = i * (numBuffers - 1);
            int end = Math.min(runs.size(), (i + 1) * (numBuffers - 1));
            result.add(
                    mergeSortedRuns(
                            runs.subList(start, end)
                    )
            );
        }
        return result;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        // create Runs
        List<Run> runs = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            BacktrackingIterator<Record> backtrackingIterator = getBlockIterator(sourceIterator, getSchema(), numBuffers);
            runs.add(sortRun(backtrackingIterator));
        }
        int numMergePass = (int) Math.ceil(
                Math.log(runs.size()) /
                        Math.log(numBuffers - 1)
        );
        for (int i = 0; i < numMergePass; i++) {
            runs = mergePass(runs);
        }
        return runs.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

