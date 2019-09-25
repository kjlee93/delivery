package com.example.template;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.BeanUtils;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.PostPersist;

@Entity
@Getter
@Setter
public class Delivery {

    @Id
    @GeneratedValue
    private Long deliveryId;
    private Long orderId;
    private String customerId;
    private String customerName;
    private String deliveryAddress;
    private String deliveryState;

    @PostPersist
    private void publishDeliveryStart() {
        KafkaTemplate kafkaTemplate = Application.applicationContext.getBean(KafkaTemplate.class);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = null;

        if (deliveryState.equals(DeliveryStarted.class.getSimpleName())) {
            DeliveryStarted deliveryStarted = new DeliveryStarted();
            deliveryStarted.setOrderId(this.getOrderId());
            try {
                BeanUtils.copyProperties(this, deliveryStarted);
                json = objectMapper.writeValueAsString(deliveryStarted);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("JSON format exception", e);
            }
        }

        if (json != null) {
            Environment env = Application.applicationContext.getEnvironment();
            String topicName = env.getProperty("eventTopic");
            ProducerRecord producerRecord = new ProducerRecord<>(topicName, json);
            kafkaTemplate.send(producerRecord);
        }
    }

}
