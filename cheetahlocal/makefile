#
# Makefile for cheetah programs
#

#JFLAGS = -g
CLASSPATH = .:./javax.json-1.0.3.jar
JFLAGS = -Xlint -cp ${CLASSPATH}
JC = javac -target 1.6 -source 1.6

NewRowStoreEng:NewRowStoreEng.java
	${JC} ${JFLAGS} NewRowStoreEng.java
NewColStoreEng:NewColStoreEng.java
	${JC} ${JFLAGS} NewColStoreEng.java
NewRowColStoreEng:NewRowColStoreEng.java
	${JC} ${JFLAGS} NewRowColStoreEng.java
NewColStoreEngParallel:NewColStoreEngParallel.java
	${JC} ${JFLAGS} NewColStoreEngParallel.java	
SingleQueryAnalyzer:SingleQueryAnalyzer.java
	${JC} ${JFLAGS} SingleQueryAnalyzer.java
LayoutFileGenerator:LayoutFileGenerator.java
	${JC} ${JFLAGS} LayoutFileGenerator.java
QueryLogGenerator:QueryLogGenerator.java
	${JC} ${JFLAGS} QueryLogGenerator.java
QueryLogGeneratorNoBench:QueryLogGeneratorNoBench.java
	${JC} ${JFLAGS} QueryLogGeneratorNoBench.java
SimpleQueryExecutor:SimpleQueryExecutor.java
	${JC} ${JFLAGS} SimpleQueryExecutor.java
StoreEngine:StoreEngine.java
	${JC} ${JFLAGS} StoreEngine.java
ColStoreEng:ColStoreEng.java
	${JC} ${JFLAGS} ColStoreEng.java
RowColStoreEng:RowColStoreEng.java
	${JC} ${JFLAGS} RowColStoreEng.java
RowStoreEng:RowStoreEng.java
	${JC} ${JFLAGS} RowStoreEng.java
ColStoreTest:ColStoreTest.java
	${JC} ${JFLAGS} ColStoreTest.java
Argo1Test:Argo1Test.java
	${JC} ${JFLAGS} Argo1Test.java
RowStoreTest:RowStoreTest.java
	${JC} ${JFLAGS} RowStoreTest.java
RowStoreSelectTest:RowStoreSelectTest.java
	${JC} ${JFLAGS} RowStoreSelectTest.java
RowStoreSelectWhereTest:RowStoreSelectWhereTest.java
	${JC} ${JFLAGS} RowStoreSelectWhereTest.java
RowColStoreSelectTest:RowColStoreSelectTest.java
	${JC} ${JFLAGS} RowColStoreSelectTest.java
RowColStoreSelectWhereTest:RowColStoreSelectWhereTest.java
	${JC} ${JFLAGS} RowColStoreSelectWhereTest.java
ColStoreSelectTest:ColStoreSelectTest.java
	${JC} ${JFLAGS} ColStoreSelectTest.java
ColStoreSelectWhereTest:ColStoreSelectWhereTest.java
	${JC} ${JFLAGS} ColStoreSelectWhereTest.java
HybridStore:HybridStore.java
	${JC} ${JFLAGS} HybridStore.java
RowColStore:RowColStore.java
	${JC} ${JFLAGS} RowColStore.java
ColStore:ColStore.java
	${JC} ${JFLAGS} ColStore.java
Argo3:Argo3.java
	${JC} ${JFLAGS} Argo3.java
Argo1:Argo1.java
	${JC} ${JFLAGS} Argo1.java
RowStore:RowStore.java
	${JC} ${JFLAGS} RowStore.java
TreeStore:TreeStore.java
	${JC} ${JFLAGS} TreeStore.java
TreeStoreTest:TreeStoreTest.java
	${JC} ${JFLAGS} TreeStoreTest.java
DataPopulator:DataPopulator.java
	${JC} ${JFLAGS} DataPopulator.java
RawCollectionsExample: RawCollectionsExample.java
	${JC} ${JFLAGS} RawCollectionsExample.java
DataBuffer: DataBuffer.java 
	${JC} ${JFLAGS} DataBuffer.java
TestBuffer: TestBuffer.java 
	${JC} ${JFLAGS} TestBuffer.java
SeqBufferEngine: SeqBufferEngine.java	User.java 
	${JC} ${JFLAGS} SeqBufferEngine.java User.java
TestTweet: TestTweet.java Tweet.java
	${JC} ${JFLAGS} TestTweet.java Tweet.java
ReadJsonUserGson: ReadJsonUseGson.java	User.java 
	${JC} ${JFLAGS} ReadJsonUseGson.java User.java
RingBufferEngine: RingBufferEngine.java	User.java 
	${JC} ${JFLAGS} RingBufferEngine.java User.java
QueryParallel: QueryParallel.java 
	${JC} ${JFLAGS} QueryParallel.java
	
clean:
	rm *.class
