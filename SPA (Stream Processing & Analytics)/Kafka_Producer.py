#!/usr/bin/env python
# coding: utf-8

# <h3>Setup Kafka Producer</h3>
# 
# Create a Kafka producer script to send time series data to a Kafka topic.

# In[2]:


from confluent_kafka import Producer
import json


# In[3]:


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# In[4]:


def produce_messages(producr, customer_data):
    serialized_data = json.dumps(customer_data)
    producr.produce(topic = "spa_assignment_topic", value=serialized_data)
    producr.flush()
    print("customer data produced")


# In[5]:


def send_data_to_kafka_via_producer(cust_data):
    
    from confluent_kafka import Producer
    import json
    
    producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'client.id': 'assignment_producer'
    }
    
    producer = Producer(producer_config)
    
    produce_messages(producer, cust_data)


# In[ ]:





# In[ ]:





# In[ ]:




