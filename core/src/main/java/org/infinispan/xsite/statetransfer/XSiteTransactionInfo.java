package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 *
 */
public class XSiteTransactionInfo {

    private final GlobalTransaction globalTransaction;

    private final WriteCommand[] modifications;


    public XSiteTransactionInfo(GlobalTransaction globalTransaction, WriteCommand[] modifications) {
        this.globalTransaction = globalTransaction;
        this.modifications = modifications;

    }

    public GlobalTransaction getGlobalTransaction() {
        return globalTransaction;
    }

    public WriteCommand[] getModifications() {
        return modifications;
    }


    @Override
    public String toString() {
        return "TransactionInfo{" +
                "globalTransaction=" + globalTransaction +

                ", modifications=" + Arrays.asList(modifications) +

                '}';
    }

    public static class Externalizer extends AbstractExternalizer<XSiteTransactionInfo> {

        @Override
        public Integer getId() {
            return Ids.TRANSACTION_INFO;
        }

        @Override
        public Set<Class<? extends XSiteTransactionInfo>> getTypeClasses() {
            return Collections.<Class<? extends XSiteTransactionInfo>>singleton(XSiteTransactionInfo.class);
        }

        @Override
        public void writeObject(ObjectOutput output, XSiteTransactionInfo object) throws IOException {
            output.writeObject(object.globalTransaction);

            output.writeObject(object.modifications);

        }

        @Override
        @SuppressWarnings("unchecked")
        public XSiteTransactionInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            GlobalTransaction globalTransaction = (GlobalTransaction) input.readObject();

            WriteCommand[] modifications = (WriteCommand[]) input.readObject();

            return new XSiteTransactionInfo(globalTransaction, modifications);
        }
    }
}
