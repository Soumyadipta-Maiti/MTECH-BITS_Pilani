Assume that you are working as analyst for “Pizzario”, a pizza delivery chain. The group has collected some interesting characteristics of customers who had purchased their pizza earlier. (Refer the attached pizza_customers.csv file for the same). The marketing team is planning a campaign to increase the sales of a newly launched pizza. Before that they want to analyze the segmentation of existing customers so that they can have the clearer picture about the customer categories.

Exercise 1: [5 marks] <br>
Write a Python program that will take the pizza customers dataset as input and produce the clusters which represents the customer segments present in the dataset. <br>
•	You may like to do some preprocessing on the given dataset. <br>
•	You have to write your own code matching to the problem statement. <br>
•	Add comments at appropriate place so that it’s easy to understand your thought process.<br>
•	You are supposed to use k-means clustering algorithm (custom implementation not from any library) for customer segmentations. <br>
•	The program should clearly output the cluster number, centroid used and number of records belonging to that cluster.<br>
•	The final clusters should be preserved in such a way that it can be used in following exercises.<br>

Your marketing team is now aware about the spending behavior of your customers. Wear hat of marketing professional and think of marketing strategy to attract these customers to your newly launched pizza. 

Exercise 2: [2 marks]

As an outcome of Exercise 1, you will obtain clusters in the given dataset. <br>
•	Apply appropriate labels to those clusters after careful look at the points belonging to those clusters. <br>
  o	Explain your logic behind nomenclature of the clusters. Show the sample dataset.  <br>
•	Based on this segmentation, think of some marketing offers that can be given to the customers. <br>
  o	Briefly explain the logic behind the offers to be made to the various customer categories. <br>

By this time you are well aware about your existing customer base. You know the customers spending habit, you have decided upon offers to be made, now it’s time to test it out for the existing customers.  Let’s assume that they frequently visits the mall where your pizza shop is located. For this purpose you have to have the mechanisms through which their visits to the mall are captured.

Exercise 3: [2 marks] <br>
Write a Python program that will simulate the movement of existing customers around a mall in near real time fashion.  <br>
•	You can assume that customer’s cell phones are enabling the transfer of location data to your centralized server where it’s stored for further analysis. <br>
With access to the real time movements of customers around mall, you are feeling more powerful and ready to target these customers. Your accumulated knowledge of streaming data processing and analytics can be leveraged for the same purpose. Now think for an architecture to bring it to the reality through a streaming data pipeline. <br>


Exercise 4: [6 marks]

Construct a streaming data pipeline integrating the various technologies, tools and Programmes covered in the course that will harvest this real time data of customer’s movements and produces the offers that can be sent on the customers mobile devices. You can think of various aspects related to streaming data processing such as: <br>
•	Real time streaming data ingestion <br>
•	Data’s intermittent storage <br>
•	Data preprocessing – cleaning, transformations etc. <br>
•	Data processing – filters, joins, windows etc. <br>
•	Business logic for placing the offers <br>
•	Final representation of the outcome <br>


