SentimentAnalyzer

1.) Build the image
    sudo docker image build -t sentimentanalyzer .
    
2.) Run the Container
    sudo docker run --network=host --env KafkaServer=192.168.1.82:9092 --env ContainerName=second_step --name=sc sentimentanalyzer
    

References:

https://kafka-python.readthedocs.io/en/master/

https://www.docker.com/blog/how-to-dockerize-your-python-applications/


 
