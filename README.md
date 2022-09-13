SentimentAnalyzer

1.) Build the image
    docker image build -t sentimanalyzer .
2.) Run the Container
    docker run --network netflow --env KafkaServer=kafkaserv:9092 --name=sentimentanalyzercontainer sentimentanalyzer
 
