#initialise the SQLContext
val sqlc = new org.apache.spark.sql.SQLContext(sc)
import sqlc.implicits._

# Read the data and remove the header
  val electionData = sc.textFile("/user/sarangdp/data/ls2014.tsv").mapPartitionsWithIndex((idx,itr)=> if(idx==0) itr.drop(1) else itr)

#filter the UP data
val upDataFilter = electionData.filter(rec => (rec.split("\t")(0)=="Uttar Pradesh"))

#Create case class
case class upData(constituency:String,partyname:String,totalVotes:Int)
  
#Get only constituency,partyname and total votes
val upDataDF = upDataFilter.map(rec => {
val r =rec.split("\t")
upData(r(1),r(6),r(10).toInt)
}).toDF

upDataDF.registerTempTable("upData")
          
val totalSeats = sqlc.sql("select partyname,count(1) noOfSeats from (select u.* from(select constituency,max(totalVotes) maxVotes "+ 
                          "from upData group by constituency) m join upData u on m.constituency = u.constituency "+
                          "and m.maxVotes = u.totalVotes) total group by partyname")
+---------+---------+                                                           
|partyname|noOfSeats|
+---------+---------+
|       AD|        2|
|      BJP|       71|
|       SP|        5|
|      INC|        2|
+---------+---------+
         
#Now consider that SP and INC fighting election together
val upDataDF = upDataFilter.map(rec => {
val r =rec.split("\t")
if(r(6)=="INC" || r(6)=="SP") upData(r(1),"ALLY",r(10).toInt) else upData(r(1),r(6),r(10).toInt)
}).toDF

upDataDF.registerTempTable("upDataTemp")
val allyVotes = sqlc.sql("select constituency,partyname, sum(totalVotes) totalVotes from upDataTemp group by constituency,partyname")
allyVotes.registerTempTable("upData")

val totalSeats = sqlc.sql("select partyname,count(1) noOfSeats from (select u.* from(select constituency,max(totalVotes) maxVotes "+
     | "from upData group by constituency) m join upData u on m.constituency = u.constituency "+
     | "and m.maxVotes = u.totalVotes) total group by partyname")
     
 totalSeats.show
 
+---------+---------+                                                           
|partyname|noOfSeats|
+---------+---------+
|       AD|        2|
|      BJP|       66|
|     ALLY|       12|
+---------+---------+
