//Read CSV file
val delayed_flights = sc.textFile("DelayedFlights.csv")

//Find out the top 5 most visited destinations
val mapping = delayed_flights.map(x => x.split(",")).map(x => (x(18),1)).filter(x => x._1!=null).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(5)

//Which month has seen the most number of cancellations due to bad weather?
val canceled = delayed_flights.map(x => x.split(",")).filter(x => ((x(22).equals("1"))&& (x(23).equals("B")))).map(x => (x(2),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(1)

//Top ten origins with the highest AVG departure delay
val avg = delayed_flights.map(x => x.split(",")).map(x => (x(17),x(16).trim.toDouble)).mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10)

//Which route (origin & destination) has seen the maximum diversion?
val diversion = delayed_flights.map(x => x.split(",")).filter(x => ((x(24).equals("1")))).map(x => ((x(17)+","+x(18)),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10).foreach(println)

