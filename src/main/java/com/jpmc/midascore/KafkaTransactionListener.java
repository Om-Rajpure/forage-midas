package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public KafkaTransactionListener(DatabaseConduit databaseConduit, RestTemplateBuilder restTemplateBuilder) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplateBuilder.build();
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
                float incentiveAmount = 0.0f;
                try {
                    Incentive incentive = restTemplate.postForObject(
                        "http://localhost:8080/incentive",
                        transaction,
                        Incentive.class
                    );
                    if (incentive != null) {
                        incentiveAmount = incentive.getAmount();
                    }
                } catch (Exception e) {
                    // Fail-safe default to 0.0f if Incentive API is down or errors out
                }

                sender.setBalance(sender.getBalance() - amount);
                recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

                databaseConduit.save(sender);
                databaseConduit.save(recipient);

                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount, incentiveAmount);
                databaseConduit.save(transactionRecord);
            }
        }
    }
}