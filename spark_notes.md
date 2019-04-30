### parseEmail pulled out messageID, Data and From with little modification

### some sender entries have several emails; had to use strip() to get the endings off
### had to use replace to get the \n and \t from within the lines

```%spark2.pyspark
# final
def parseEmail(email):
    fields = email.split("Subject:")[0].split('\n')
    messageID = fields[0].split(':')[1]
    date = fields[1].split(':')[1]
    sender = fields[2].split(':')[1]
    message = email.replace('\n','').replace('\t','')
    
    
    receivers = email.split("To:")[1].split('Subject:')[0].split(',')
    clean_receivers = [i.strip() for i in receivers]
    to = ",".join(clean_receivers)
    return([messageID,date,sender,to,message])
    
schema = StructType([
    StructField("MessageID", StringType()),
    StructField("Date", StringType()),
    StructField("From", StringType()),
    StructField("To", StringType()),
    StructField("Message", StringType())])


mydata = sc.wholeTextFiles('file:///data/maildir/*/*/*')
emails = mydata.map(lambda x: x[1]).map(lambda x: parseEmail(x))

spark.createDataFrame(emails, schema=schema).write.format('csv').options(header='true').save('file:///tmp/enron')```