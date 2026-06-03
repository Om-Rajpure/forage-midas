package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private final DatabaseConduit databaseConduit;

    public KafkaTransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-group"
    )
    public void listen(Transaction transaction) {
        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            float amount = transaction.getAmount();
            if (sender.getBalance() >= amount) {
                sender.setBalance(sender.getBalance() - amount);
                recipient.setBalance(recipient.getBalance() + amount);

                databaseConduit.save(sender);
                databaseConduit.save(recipient);

                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount);
                databaseConduit.save(transactionRecord);
            }
        }
    }
}