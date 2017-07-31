package org.corfudb.generator.operations;

import org.corfudb.generator.State;

/**
 *
 * A base generic operation that the generator can execute.
 */
public class BaseOperation {
    protected State state;
    private Operation op;
    private String invokeDescription = null;
    private String resultDescription = null;
    long invokeTime = -1;

    public BaseOperation() {
    }

    public BaseOperation(State state, Operation op) {
        this.state = state;
        this.op = op;
    }

    public void execute() {
        invokeTime = state.getSystemTime();
        try {
            op.execute();
            System.out.printf("{:time %d :type :invoke :f :txn :value [%s] :process %s}\n",
                    invokeTime, invokeDescription, Thread.currentThread().getName());
            System.out.printf("{:time %d :type :invoke :f :txn :value [%s] :process %s}\n",
                    state.getSystemTime(), resultDescription, Thread.currentThread().getName());
        } catch (Exception e) {
            System.out.printf("Exception type %s:\n", e.getClass());
            e.printStackTrace();
            System.out.printf("{:time %d :type :invoke :f :txn :value [%s] :process %s}\n",
                    invokeTime, invokeDescription, Thread.currentThread().getName());
            System.out.printf("{:time %d :error :%s :f :txn :value [%s] :process %s}\n",
                    state.getSystemTime(), e.getClass(), "   ", Thread.currentThread().getName());
        }
    }

    public void appendInvokeDescription(String s) {
        if (invokeDescription == null) {
            invokeDescription = s;
        } else {
            invokeDescription = invokeDescription + " " + s;
        }
    }

    public void appendResultDescription(String s) {
        if (invokeDescription == null) {
            invokeDescription = s;
        } else {
            invokeDescription = invokeDescription + " " + s;
        }
    }

    public void foo() {
        /***
        System.err.printf("-------- {:time %d  :type :invoke :f :txn :value [[:write %s %s]] :process %s }\n",
                invokeTime, fullKey, value, Thread.currentThread().getName());
         ***/

    }
}
