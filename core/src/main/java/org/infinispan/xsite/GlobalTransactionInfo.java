package org.infinispan.xsite;

import org.infinispan.transaction.xa.GlobalTransaction;

/**
*
 */
public class GlobalTransactionInfo {
      public enum TransactionStatus {

      PREPARED_RECEIVED,
      COMMIT_RECEIVED,

    }
    private TransactionStatus transactionStatus;
    private GlobalTransaction globalTransaction;

    public GlobalTransactionInfo(GlobalTransaction globalTransaction, TransactionStatus transactionStatus){
        this.globalTransaction = globalTransaction;
        this.transactionStatus = transactionStatus;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public GlobalTransaction getGlobalTransaction() {
        return globalTransaction;
    }
}
